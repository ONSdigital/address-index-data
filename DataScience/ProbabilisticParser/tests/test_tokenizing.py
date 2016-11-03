"""
ONS Address Index - Probabilistic Parser Tokenizing Test
========================================================

A few unit tests to check that the tokenizer splits the data correctly and
some of the feature and metric functions work correctly.


Author
------

:author: Sami Niemi (sami.niemi@valtech.co.uk)


Version
-------

:version: 0.2
:date: 3-Nov-2016
"""
import ProbabilisticParser.common.tokens as tokens
import ProbabilisticParser.common.metrics as metrics
import unittest


class TestTokenizing(unittest.TestCase):

    def test_split_on_punc(self):
        assert tokens.tokenize('foo,bar') == ['foo,', 'bar']

    def test_real_addresses(self):
        assert tokens.tokenize('CHERRY TREE HOUSING ASSOCIATION 5 TAVISTOCK AVENUE ST ALBANS AL1 2NQ') \
        == ['CHERRY', 'TREE', 'HOUSING', 'ASSOCIATION', '5', 'TAVISTOCK', 'AVENUE', 'ST', 'ALBANS', 'AL1', '2NQ']
        assert tokens.tokenize('339 PERSHORE ROAD EDGBASTON BIRMINGHAM B5 7RY') == ['339', 'PERSHORE', 'ROAD',
                                                                                    'EDGBASTON', 'BIRMINGHAM', 'B5',
                                                                                    '7RY']

    def test_spaces(self):
        assert tokens.tokenize('foo bar') == ['foo', 'bar']
        assert tokens.tokenize('foo  bar') == ['foo', 'bar']
        assert tokens.tokenize('foo bar ') == ['foo', 'bar']
        assert tokens.tokenize(' foo bar') == ['foo', 'bar']


class TestDigits(unittest.TestCase):

    def test_digits(self):
        assert tokens.digits('street') == 'no_digits'
        assert tokens.digits('12b') == 'some_digits'
        assert tokens.digits('12324') == 'all_digits'


class TestMetrics(unittest.TestCase):

    def test_sequence_accuracy(self):
        assert metrics.sequence_accuracy_score(['a', 'b', 'c', 'd'], ['a', 'b', 'c', 'd']) == 1
        assert metrics.sequence_accuracy_score(['a', 'b', 'c', 'd'], ['a', 'b', 'c', 'f']) == 0.75
        assert metrics.sequence_accuracy_score(['a', 'b', 'c', 'd'], ['a', 'b', 'a', 'b']) == 0.5
        assert metrics.sequence_accuracy_score(['a', 'b', 'c', 'd'], ['a', 'b', 'c', 'd'][::-1]) == 0.


if __name__ == '__main__':
    unittest.main(verbosity=2)
