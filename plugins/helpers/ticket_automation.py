"""Shared, bounded ticket classification and approved action-plan selection."""

import re

EMAIL = re.compile(r'[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}')
MSISDN = re.compile(r'\b(?:MDN|MSISDN)\b\s*[:=#-]?\s*([0-9]{7,15})(?![0-9])', re.I)
BAN = re.compile(r'\bBAN\b\s*[:=#-]?\s*([0-9]{1,30})', re.I)
USER_ID = re.compile(r'\bU-[A-Za-z0-9\-]+')


def extract_facts(ticket, recipe):
    """Match the production script's precedence while bounding untrusted text."""
    summary = str(ticket.get('name') or '')[:4096]
    description = str(ticket.get('description') or '')[:16384]
    custom = ticket.get('custom_fields') or {}
    if not isinstance(custom, dict):
        custom = {}
    custom = {str(k).strip().lower(): str(v)[:4096] for k, v in custom.items() if isinstance(v, str)}
    email_candidates = {match.lower() for source in (description, summary, *custom.values())
        for match in EMAIL.findall(source)}
    preferred_key = recipe['extraction']['preferred_email_key']
    preferred = EMAIL.search(custom.get(preferred_key, ''))
    email = preferred or next((match for source in (description, summary, *custom.values())
        if (match := EMAIL.search(source))), None)
    def match(pattern, sources):
        return next((found.group(1) if found.lastindex else found.group(0)
            for source in sources if (found := pattern.search(source))), None)
    custom_mdn = re.compile(r'(?<![0-9])([0-9]{7,15})(?![0-9])')
    line_candidates = set(MSISDN.findall(summary) + MSISDN.findall(description)
        + custom_mdn.findall(custom.get('msisdn', '')))
    msisdn = (match(MSISDN, (summary, description)) or match(custom_mdn, (custom.get('msisdn', ''),)))
    ban = match(BAN, (summary, description)) or match(re.compile(r'([0-9]{1,30})'), (custom.get('ban', ''),))
    return {'email': email.group(0) if email and len(email_candidates) == 1 else None,
        'msisdn': msisdn if len(line_candidates) <= 1 else None, 'ban': ban,
        'user_id': match(USER_ID, (summary, description, *custom.values())),
        'ambiguous_email': len(email_candidates) > 1 or bool(ticket.get('custom_fields_invalid')),
        'ambiguous_msisdn': len(line_candidates) > 1 or bool(ticket.get('custom_fields_invalid'))}


def classification_input(ticket, facts, recipe):
    """Only derived categories reach Decision; no free ticket or IAM text."""
    summary = str(ticket.get('name') or '')
    return {'ticket_kind': recipe['id'] if summary.startswith(
        recipe['ticket_filter']['short_description_prefix']) else 'other',
        'has_email': bool(facts.get('email')), 'has_msisdn': bool(facts.get('msisdn')),
        'has_ban': bool(facts.get('ban')), 'has_user_id': bool(facts.get('user_id'))}


def select_plan(recipe, summary, decision, registered_actions):
    """Return (plan id, source), None for no match, or raise on unsafe ambiguity."""
    if not isinstance(decision, dict):
        raise ValueError('Decision response invalid')
    plans = recipe['plans']
    if not isinstance(plans, dict) or any(not isinstance(actions, list) or not actions or
        any(action not in registered_actions for action in actions) for actions in plans.values()):
        raise ValueError('Recipe references unregistered actions')
    if decision and decision.get('status') == 'selected':
        choice = decision.get('choice')
        if choice not in plans:
            raise ValueError('Decision selected unknown plan')
        return choice, 'decision'
    if decision and decision.get('status') not in ('abstained', 'unavailable'):
        raise ValueError('Decision returned unknown status')
    matches = []
    for fallback in recipe.get('fallback_patterns', []):
        pattern, plan = fallback['pattern'], fallback['plan']
        if not isinstance(pattern, str) or len(pattern) > 256 or plan not in plans:
            raise ValueError('Invalid regex fallback')
        if re.search(pattern, str(summary)[:4096], re.I):
            matches.append(plan)
    if len(matches) > 1:
        raise ValueError('Ambiguous regex fallback')
    return (matches[0], 'regex') if matches else None


def select_output_policy(recipe, action_outcomes):
    rules = recipe.get('response_rules') or []
    if not rules:
        return recipe['outputs']
    for action in reversed(action_outcomes):
        matches = [rule['outputs'] for rule in rules
            if rule['action'] == action and action_outcomes[action] == rule['outcome']]
        if len(matches) > 1:
            raise ValueError('Ambiguous response rules')
        if matches:
            return matches[0]
    return None
