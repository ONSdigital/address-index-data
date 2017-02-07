"""
ONS Address Index - Probabilistic Parser
========================================

This file defines the calling mechanism for a trained probabilistic parser model.
It also implements a simple test. Note that the results are model dependent, so
the assertions will fail if a new model is trained.


Requirements
------------

:requires: pycrfsuite (https://python-crfsuite.readthedocs.io/en/latest/)


Author
------

:author: Sami Niemi (sami.niemi@valtech.co.uk)


Version
-------

:version: 0.3
:date: 7-Feb-2017
"""
import os
import sys
from collections import OrderedDict

import ProbabilisticParser.common.tokens as tok
import pycrfsuite

try:
    TAGGER = pycrfsuite.Tagger()
    TAGGER.open(tok.MODEL_PATH + tok.MODEL_FILE)
    print('Using model from', tok.MODEL_PATH + tok.MODEL_FILE)
except IOError:
    print('ERROR: cannot find the CRF model file', tok.MODEL_FILE, 'from', tok.MODEL_PATH)
    sys.exit(-9)


def parse(raw_string):
    """
    Parse the given input string using a trained model. Returns a list of tokens and labels.

    :param raw_string: input string to parse
    :type raw_string: str

    :return: a list of tokens and labels
    :rtype: list
    """
    tokens = tok.tokenize(raw_string)
    if not tokens:
        return []

    features = tok.tokens2features(tokens)

    tags = TAGGER.tag(features)

    return list(zip(tokens, tags))


def parse_with_marginal_probability(raw_string):
    """
    Parse the given input string using a trained model.
    Returns a list of tokens, labels, and marginal probabilities.

    :param raw_string: input string to parse
    :type raw_string: str

    :return: a list of tokens, labels, and marginal probabilities
    :rtype: list
    """
    tokens = tok.tokenize(raw_string)
    if not tokens:
        return []

    features = tok.tokens2features(tokens)

    tags = TAGGER.tag(features)

    marginals = [TAGGER.marginal(tag, i) for i, tag in enumerate(tags)]

    return list(zip(tokens, tags, marginals))


def parse_with_probabilities(raw_string):
    """
    Parse the given input string using a trained model.
    Returns a dictionary with

    :param raw_string: input string to parse
    :type raw_string: str

    :return: a dictionary holding the results
    :rtype: OrderedDict
    """
    tokens = tok.tokenize(raw_string)
    if not tokens:
        return []

    features = tok.tokens2features(tokens)

    tags = TAGGER.tag(features)

    marginals = [TAGGER.marginal(tag, i) for i, tag in enumerate(tags)]
    sequence_probability = TAGGER.probability(tags)

    out = OrderedDict(tokens=tokens, tags=tags, marginal_probabilites=marginals,
                      sequence_probability=sequence_probability)

    return out


def tag(raw_string):
    """
    Parse the given input string using a trained model. Returns an ordered dictionary of tokens and labels.
    Unlike the parse function returns a complete label i.e. joins multiple labels to a single string and
    labels the full string given the label.

    :param raw_string: input string to parse and label
    :type raw_string: str

    :return: a dictionary of tokens and labels
    :rtype: Ordered Dictionary
    """
    tagged = OrderedDict()

    for token, label in parse(raw_string):
        tagged.setdefault(label, []).append(token)

    for token in tagged:
        component = ' '.join(tagged[token])
        component = component.strip(' ,;')
        tagged[token] = component

    return tagged


def test(raw_string='ONS LIMITED FLAT 1 12 OXFORD STREET STREET ST1 2FW', verbose=False):
    """
    A simple test to check that the calling mechanism from Python gives the same
    results as if CRFsuite were called directly from the command line. Requires
    a compiled version of the CRFsuite.

    :param raw_string: input string to test
    :type raw_string: str
    :param verbose: additional debugging output
    :type verbose: bool

    :return: None
    """
    print('Input string:', raw_string)
    print('Python Results:', tag(raw_string))

    tokens = tok.tokenize(raw_string)
    features = tok.tokens2features(tokens)

    if verbose:
        print('features:', features)

    tags = TAGGER.tag(features)
    print('Inferred tags:', tags)

    print('Probability of the sequence:', round(TAGGER.probability(tags), 6))
    assert round(TAGGER.probability(tags), 6) == 0.992256, 'Sequence probability not correct'

    results = [0.999999, 0.999999, 0.999846, 0.993642, 0.999728, 1., 1., 0.998874, 1., 1.]
    for i, tg in enumerate(tags):
        prob = round(TAGGER.marginal(tg, i), 6)
        print('Marginal probability of', tg, 'in position', i, 'is', prob)
        assert prob == results[i], 'Marginal Probability of a Label not correct'

    if verbose:
        print(TAGGER.info().transitions)
        print(TAGGER.info().state_features)
        print(TAGGER.info().attributes)

    # store the ItemSequence temporarily
    tmp = pycrfsuite.ItemSequence(features)

    # write to a text file
    fh = open('training/test.txt', 'w')
    for i, tg in enumerate(tags):
        fh.write(tg + '\t')
        items = tmp.items()[i]
        for item in sorted(items):
            itemtext = str(item)
            fh.write(itemtext.replace(':', '\:') + ':' + str(items[item]) + '\t')
        fh.write('\n')
    fh.close()

    # command line call to the C code to test the output
    print('\nCRFsuite call results:')
    os.system('crfsuite tag -pit -m training/addressCRF.crfsuite training/test.txt')


if __name__ == "__main__":
    test()
