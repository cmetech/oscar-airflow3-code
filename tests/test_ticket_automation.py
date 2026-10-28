import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parents[1] / 'plugins'))
from helpers.ticket_automation import extract_facts, select_plan, classification_input, select_output_policy


RECIPE = {
    'id': 'metro_unlink',
    'ticket_filter': {'short_description_prefix': '[TMOID] [Metro Linked]'},
    'extraction': {'preferred_email_key': 'what t-mobile id is the customer using to sign into t-life app?'},
    'plans': {'metro_unlink': ['iam.lookup_user', 'iam.unlink_metro_line']},
    'fallback_patterns': [{'pattern': r'^\[TMOID\] \[Metro Linked\]', 'plan': 'metro_unlink'}],
}


def test_extraction_preserves_production_precedence():
    ticket = {'name': '[TMOID] [Metro Linked] U-123 MDN 5551234567',
        'description': 'BAN 1234 mail other@example.test',
        'custom_fields': {'what t-mobile id is the customer using to sign into t-life app?': 'preferred@example.test',
            'msisdn': '9997776666'}}
    facts = extract_facts(ticket, RECIPE)
    assert facts['email'] is None and facts['ambiguous_email'] is True
    assert facts['msisdn'] is None and facts['ambiguous_msisdn'] is True
    assert facts['ban'] == '1234' and facts['user_id'] == 'U-123'
    assert 'preferred@example.test' not in str(classification_input(ticket, facts, RECIPE))
    assert '5551234567' not in str(classification_input(ticket, facts, RECIPE))


def test_decision_and_unique_fallback_select_registered_plan():
    assert select_plan(RECIPE, '[TMOID] [Metro Linked] Request', {'status': 'selected', 'choice': 'metro_unlink'},
        {'iam.lookup_user', 'iam.unlink_metro_line'}) == ('metro_unlink', 'decision')
    assert select_plan(RECIPE, '[TMOID] [Metro Linked] Request', {'status': 'abstained'},
        {'iam.lookup_user', 'iam.unlink_metro_line'}) == ('metro_unlink', 'regex')
    assert select_plan(RECIPE, 'Other', {'status': 'abstained'}, {'iam.lookup_user', 'iam.unlink_metro_line'}) is None


def test_unknown_or_ambiguous_choice_fails_closed():
    with pytest.raises(ValueError):
        select_plan(RECIPE, 'Other', ['invalid'], {'iam.lookup_user', 'iam.unlink_metro_line'})
    with pytest.raises(ValueError):
        select_plan(RECIPE, 'Other', {'status': 'selected', 'choice': 'unknown'}, {'iam.lookup_user', 'iam.unlink_metro_line'})
    ambiguous = {**RECIPE, 'fallback_patterns': RECIPE['fallback_patterns'] * 2}
    with pytest.raises(ValueError):
        select_plan(ambiguous, '[TMOID] [Metro Linked] Request', {'status': 'abstained'},
            {'iam.lookup_user', 'iam.unlink_metro_line'})


def test_conflicting_emails_and_oversized_line_are_never_selected():
    facts = extract_facts({'name': '[TMOID] [Metro Linked] MDN 12345678901234567890',
        'description': 'one@example.test and two@example.test', 'custom_fields': {}}, RECIPE)
    assert facts['email'] is None and facts['ambiguous_email'] is True
    assert facts['msisdn'] is None
    competing = extract_facts({'name': '[TMOID] [Metro Linked] MDN 5551234567',
        'description': 'MSISDN 5557654321 person@example.test', 'custom_fields': {}}, RECIPE)
    assert competing['msisdn'] is None and competing['ambiguous_msisdn'] is True


def test_response_rules_choose_only_one_sanitized_outcome():
    recipe = {**RECIPE, 'outputs': {'ticket_note': None, 'email': None},
        'response_rules': [{'action': 'iam.unlink_metro_line', 'outcome': 'accepted',
            'outputs': {'ticket_note': 'approved.j2', 'email': None}}]}
    assert select_output_policy(recipe, {'iam.unlink_metro_line': 'accepted'})['ticket_note'] == 'approved.j2'
    assert select_output_policy(recipe, {'iam.unlink_metro_line': 'stubbed'}) is None
    with pytest.raises(ValueError):
        select_output_policy({**recipe, 'response_rules': recipe['response_rules'] * 2},
            {'iam.unlink_metro_line': 'accepted'})
