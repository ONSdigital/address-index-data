from addressParser import tokenize
import unittest


class TestTokenizing(unittest.TestCase):

    def test_split_on_punc(self):
        assert tokenize('foo,bar') == ['foo,', 'bar']

    def test_real_addresses(self):
        assert tokenize('CHERRY TREE HOUSING ASSOCIATION 5 TAVISTOCK AVENUE ST ALBANS AL1 2NQ') \
        == ['CHERRY', 'TREE', 'HOUSING', 'ASSOCIATION', '5', 'TAVISTOCK', 'AVENUE', 'ST', 'ALBANS', 'AL1', '2NQ']
        assert tokenize('339 PERSHORE ROAD EDGBASTON BIRMINGHAM B5 7RY') == ['339', 'PERSHORE', 'ROAD', 'EDGBASTON',
                                                                             'BIRMINGHAM', 'B5', '7RY']

    def test_spaces(self):
        assert tokenize('foo bar') == ['foo', 'bar']
        assert tokenize('foo  bar') == ['foo', 'bar']
        assert tokenize('foo bar ') == ['foo', 'bar']
        assert tokenize(' foo bar') == ['foo', 'bar']


if __name__ == '__main__':
    unittest.main()    
