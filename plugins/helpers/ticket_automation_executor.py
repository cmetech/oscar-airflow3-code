"""One bounded ticket-automation pass; the DAG supplies approved handlers and hooks."""

import json
from contextlib import nullcontext

import httpx
import yaml

from helpers.ticket_automation import classification_input, extract_facts, select_plan, select_output_policy


def eligible(current, ingested, recipe):
    filters = recipe['ticket_filter']
    return (current.get('sys_id') == ingested.get('sys_id') and
        current.get('incident_number') == ingested.get('incident_number') and
        ('created_by' not in ingested or current.get('created_by') == ingested.get('created_by')) and
        current.get('status') in ('new', 'in_progress', 'on_hold') and
        current.get('assigned_group') == filters['assigned_group'] and
        str(current.get('name') or '').startswith(filters['short_description_prefix']))


def identity_matches(current, facts, recipe):
    fresh = extract_facts(current, recipe)
    return not fresh.get('ambiguous_email') and not fresh.get('ambiguous_msisdn') and all(
        fresh.get(key) == facts.get(key) for key in ('email', 'msisdn', 'user_id', 'ban'))


def _safe_log(worklog, incident_number, event):
    if worklog is not None:
        worklog.info(f'Incident {incident_number}: {event}')


def run_once(tickets, decide, state, registry, recipe, raw_recipe, *, mapping,
             effect_key, render_note, outcome_unknown, lookup_not_found,
             live_writes=False, worklog=None, worklog_factory=None, notify=None):
    """Ingest by keyset, pin recipe, and execute semantic effects at most once."""
    counts = {'ingested': 0, 'processed': 0, 'manual_review': 0}
    cursor = None
    consecutive_read_failures = 0
    while True:
        filters = {key: recipe['ticket_filter'][key] for key in ('assigned_group', 'active_only')}
        if cursor:
            filters['after_sys_id'] = cursor
        page = tickets.search_tickets(filters, per_page=100, system='snow',
            custom_field_keys=recipe['custom_field_keys'])
        if not isinstance(page, list) or len(page) > 100:
            raise RuntimeError('Ticket search response invalid')
        if not page:
            break
        for ticket in page:
            sid = ticket.get('sys_id')
            if not isinstance(sid, str) or len(sid) != 32 or (cursor and sid <= cursor):
                raise RuntimeError('Ticket search order invalid')
            cursor = sid
            if not eligible(ticket, ticket, recipe):
                continue
            counts['ingested'] += 1
            row = state.ingest(ticket, raw_recipe, recipe, extract_facts(ticket, recipe))
            if row['status'] in ('complete', 'manual_review'):
                continue
            with (worklog_factory(ticket['incident_number']) if worklog_factory else nullcontext(worklog)) as ticket_worklog:
                pinned = yaml.safe_load(row['recipe_snapshot'])
                facts = json.loads(row['facts_json']) if isinstance(row['facts_json'], str) else row['facts_json']
                if not isinstance(pinned, dict) or not isinstance(facts, dict):
                    raise RuntimeError('Pinned ticket state invalid')
                if facts.get('ambiguous_email') or facts.get('ambiguous_msisdn'):
                    state.set_status(sid, 'manual_review')
                    counts['manual_review'] += 1
                    _safe_log(ticket_worklog, ticket['incident_number'], 'conflicting identity; manual review')
                    continue
                ticket_live_writes = live_writes and pinned['stub_writes'] is False
                if ticket_live_writes and ticket.get('created_by') not in pinned['approved_creators']:
                    state.set_status(sid, 'manual_review')
                    counts['manual_review'] += 1
                    _safe_log(ticket_worklog, ticket['incident_number'], 'unapproved ticket creator; manual review')
                    continue
                saved_mapping = row.get('mapping_snapshot')
                if isinstance(saved_mapping, str):
                    saved_mapping = json.loads(saved_mapping)
                pinned_mapping = saved_mapping or mapping
                number = ticket['incident_number']
                decision = {}
                if row.get('selected_plan'):
                    choice = (row['selected_plan'], 'pinned')
                else:
                    try:
                        decision = decide(pinned['decision_id'], classification_input(ticket, facts, pinned))
                    except httpx.HTTPStatusError as error:
                        if error.response.status_code != 503:
                            raise
                        decision = {'status': 'unavailable'}
                    except httpx.RequestError:
                        decision = {'status': 'unavailable'}
                    try:
                        choice = select_plan(pinned, ticket['name'], decision,
                            set(registry))
                    except ValueError:
                        choice = None
                if choice is None:
                    state.set_status(sid, 'manual_review')
                    counts['manual_review'] += 1
                    _safe_log(ticket_worklog, number, 'no unique approved plan; manual review')
                    continue
                plan, source = choice
                plan = state.pin_plan(sid, plan)
                state.record_selection(sid, decision.get('definition_revision'), pinned_mapping)
                actions = pinned['plans'][plan]
                _safe_log(ticket_worklog, number, f'recipe {pinned["id"]}@{pinned["revision"]}; plan {plan} via {source}; actions {",".join(actions)}')
                if len(actions) == 1 and registry[actions[0]]['kind'] == 'noop':
                    state.set_status(sid, 'complete')
                    counts['processed'] += 1
                    continue
                current = tickets.get_ticket(sid, system='snow', custom_field_keys=pinned['custom_field_keys'])
                if not eligible(current, ticket, pinned) or not identity_matches(current, facts, pinned):
                    state.set_status(sid, 'manual_review')
                    counts['manual_review'] += 1
                    _safe_log(ticket_worklog, number, 'ticket drift; manual review')
                    continue
                context = row.get('action_context')
                if isinstance(context, str):
                    context = json.loads(context)
                action_outcomes = {}
                read_unavailable = False
                for action in actions:
                    handler = registry[action]
                    if handler['kind'] != 'read':
                        continue
                    if any(not facts.get(key) for key in handler.get('required_facts', ())):
                        context = None
                        state.set_status(sid, 'manual_review')
                        counts['manual_review'] += 1
                        _safe_log(ticket_worklog, number, 'required identity absent; manual review')
                        break
                    if context is None:
                        try:
                            context = handler['call'](facts, pinned_mapping, ticket_live_writes)
                        except lookup_not_found:
                            context = None
                        except RuntimeError:
                            read_unavailable = True
                            _safe_log(ticket_worklog, number, 'read action unavailable; pending retry')
                            break
                    if context is None:
                        state.set_status(sid, 'manual_review')
                        counts['manual_review'] += 1
                        _safe_log(ticket_worklog, number, 'approved lookup absent; manual review')
                        break
                    context = state.pin_action_context(sid, context)
                    action_outcomes[action] = 'found'
                if read_unavailable:
                    consecutive_read_failures += 1
                    if consecutive_read_failures >= 3:
                        return counts
                    continue
                if action_outcomes:
                    consecutive_read_failures = 0
                if any(registry[action]['kind'] == 'read' for action in actions) and context is None:
                    continue
                action_failed = False
                for action in actions:
                    handler = registry[action]
                    if handler['kind'] == 'read':
                        continue
                    if handler['kind'] != 'write':
                        raise ValueError('Unregistered action in plan')
                    key = effect_key('snow', sid, action, handler['target'](context), stub=not ticket_live_writes)
                    if state.claim_effect(key, sid, action):
                        try:
                            current = tickets.get_ticket(sid, system='snow', custom_field_keys=pinned['custom_field_keys'])
                            if not eligible(current, ticket, pinned) or not identity_matches(current, facts, pinned):
                                raise ValueError('ticket drift')
                            receipt = handler['call'](context, ticket_live_writes)
                        except outcome_unknown:
                            state.finish_effect(key, 'outcome_unknown')
                        except Exception:
                            state.finish_effect(key, 'definitely_failed')
                        else:
                            state.finish_effect(key, 'succeeded', {'outcome': receipt['outcome']})
                            _safe_log(ticket_worklog, number, f'{action} {receipt["outcome"]}')
                    if state.effect_status(key) != 'succeeded':
                        action_failed = True
                        break
                    action_outcomes[action] = (state.effect_receipt(key) or {}).get('outcome')
                if action_failed:
                    state.set_status(sid, 'manual_review')
                    counts['manual_review'] += 1
                    _safe_log(ticket_worklog, number, 'action effect needs reconciliation')
                    continue
                outputs = select_output_policy(pinned, action_outcomes)
                if outputs is None:
                    state.set_status(sid, 'manual_review')
                    counts['manual_review'] += 1
                    _safe_log(ticket_worklog, number, 'no unique response rule; manual review')
                    continue
                template = outputs.get('ticket_note')
                if template:
                    note_key = effect_key('snow', sid, 'ticket_note', template, stub=not ticket_live_writes)
                    if state.claim_effect(note_key, sid, 'ticket_note'):
                        write_started = False
                        try:
                            fresh = tickets.get_ticket(sid, system='snow', custom_field_keys=pinned['custom_field_keys'])
                            if not eligible(fresh, ticket, pinned) or not identity_matches(fresh, facts, pinned):
                                raise ValueError('ticket drift')
                            note = render_note(template, incident_number=number,
                                outcome='accepted' if ticket_live_writes else 'stubbed')
                            if ticket_live_writes:
                                write_started = True
                                tickets.update_ticket(sid, {'name': fresh['name'], 'work_info_notes': note}, system='snow')
                        except Exception:
                            state.finish_effect(note_key, 'outcome_unknown' if write_started else 'definitely_failed')
                        else:
                            state.finish_effect(note_key, 'succeeded', {'outcome': 'sent' if ticket_live_writes else 'stubbed'})
                    if state.effect_status(note_key) != 'succeeded':
                        state.set_status(sid, 'manual_review')
                        counts['manual_review'] += 1
                        continue
                email = outputs.get('email')
                if email:
                    target = json.dumps([email['notifier'], email['recipients'], email['template']], separators=(',', ':'))
                    mail_key = effect_key('snow', sid, 'email', target, stub=not ticket_live_writes)
                    if state.claim_effect(mail_key, sid, 'email'):
                        write_started = False
                        try:
                            fresh = tickets.get_ticket(sid, system='snow', custom_field_keys=pinned['custom_field_keys'])
                            if not eligible(fresh, ticket, pinned) or not identity_matches(fresh, facts, pinned):
                                raise ValueError('ticket drift')
                            message = render_note(email['template'], incident_number=number,
                                outcome='accepted' if ticket_live_writes else 'stubbed')
                            if ticket_live_writes:
                                if notify is None:
                                    raise ValueError('Approved notifier unavailable')
                                write_started = True
                                notify.send_notification({'name': email['notifier'],
                                    'recipients': email['recipients'], 'subject': 'Ticket automation for ' + number,
                                    'message': message})
                        except Exception:
                            state.finish_effect(mail_key, 'outcome_unknown' if write_started else 'definitely_failed')
                        else:
                            state.finish_effect(mail_key, 'succeeded', {'outcome': 'sent' if ticket_live_writes else 'stubbed'})
                    if state.effect_status(mail_key) != 'succeeded':
                        state.set_status(sid, 'manual_review')
                        counts['manual_review'] += 1
                        continue
                state.set_status(sid, 'complete')
                counts['processed'] += 1
    return counts
