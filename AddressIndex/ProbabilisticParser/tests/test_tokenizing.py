from addressParser import tokenize
import unittest

class TestTokenizing(unittest.TestCase) :

    def test_split_on_punc(self) :

        assert tokenize('foo,bar') == ['foo,', 'bar']
    
    def test_spaces(self) :

        assert tokenize('foo bar') == ['foo', 'bar']
        assert tokenize('foo  bar') == ['foo', 'bar']
        assert tokenize('foo bar ') == ['foo', 'bar']
        assert tokenize(' foo bar') == ['foo', 'bar']

if __name__ == '__main__' :
    unittest.main()    
