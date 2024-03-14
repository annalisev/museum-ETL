
from consumer import check_messages, convert_time


def test_check_data():
    '''Tests check_messages function.'''
    msg = {'at': '2024-03-12T17:44:19.854446+00:00', 'site': '10', 'val': 0}
    assert check_messages(msg) == 'INVALID: Invalid exhibition.'


def test_check_data_two():
    '''Tests check_messages function.'''
    msg = {'at': '2024-03-12T12:19:27.402578+00:00', 'site': '1', 'val': 8}
    assert check_messages(msg) == 'INVALID: Invalid value.'


def test_check_data_three():
    '''Tests check_messages function.'''
    msg = {"site": "2", "val": 1}
    assert check_messages(msg) == 'INVALID: Incomplete data.'


def test_check_data_four():
    '''Tests check_messages function.'''
    msg = {'at': '2024-03-12T17:44:48.856883+00:00',
           'site': '3', 'val': -1, 'type': 'INF'}
    assert check_messages(msg) == 'INVALID: Invalid type.'


def test_convert_time():
    '''Tests check_messages function.'''
    msg = {'at': '2024-0+00:00',
           'site': '3', 'val': -1, 'type': 1}
    assert convert_time(msg)[1] == 'INVALID: Time is in the wrong format.'


def test_convert_time_two():
    '''Tests check_messages function.'''
    msg = {'at': '2024-03-12T20:44:48.856883+00:00',
           'site': '3', 'val': -1, 'type': 1}
    assert convert_time(
        msg)[1] == 'INVALID: Interaction recorded while museum closed.'
