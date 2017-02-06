#!/usr/bin/env python
"""
ONS Address Index - Optimise the Probabilistic Parser
=====================================================

A simple script to train a linear-chain CRF model.

Supports both L-BFGS and AP training. Technically,
given that AP is not a maximum likelihood algorithm,
the AP trained model should not be called a CRF, but
rather something like Maximum Margin Random Fields.
In practice, most pairwise models tend to be called
CRFs independent of the training method. Thus, CRFs
they are...


Requirements
------------

:requires: sklearn-crfsuite (http://sklearn-crfsuite.readthedocs.io/en/latest/index.html)


Running
-------

After all requirements are satisfied and the training and holdout XML files have been created,
the script can be invoked using CPython interpreter::

    python trainModel.py


Author
------

:author: Sami Niemi (sami.niemi@valtech.co.uk)


Version
-------

:version: 0.3
:date: 6-Feb-2017
"""
import ProbabilisticParser.common.metrics as metric
import ProbabilisticParser.common.tokens as tkns
import sklearn_crfsuite
from sklearn_crfsuite import metrics


def read_data(training_data_file='/Users/saminiemi/Projects/ONS/AddressIndex/data/training/training1M.xml',
              holdout_data_file='/Users/saminiemi/Projects/ONS/AddressIndex/data/training/holdout.xml',
              verbose=True):
    """
    Read in the training and holdout data from XML files.

    :param training_data_file: location of the training data
    :type training_data_file: str
    :param holdout_data_file: location of the holdout data
    :type holdout_data_file: str
    :param verbose: whether or not to print to stdout
    :type verbose: bool

    :return: training data and labels, holdout data and labels
    :rtype: list
    """
    if verbose:
        print('Read in training data...')
    X_train, y_train = tkns.readData(training_data_file)

    if verbose:
        print('Read in holdout data...')
    X_test, y_test = tkns.readData(holdout_data_file)

    return X_train, y_train, X_test, y_test


def train_new_model(X_train, y_train, X_test, y_test, LBFGS=True):
    """
    Train a linear-chain Conditional Random Fields model using the input training data and labels.
    Calculates the performance on the given holdout data.

    :param X_train: training data in 2D array
    :param y_train: training data labels
    :param X_test: holdout data in 2D array
    :type y_test: holdout data true labels
    :param LBFGS:

    :return: None
    """
    print('Start training a CRF model...')

    if LBFGS:
        # note that the values for the regularisation terms have been optimised using a smaller dataset
        crf = sklearn_crfsuite.CRF(algorithm='lbfgs',
                                   c1=0.25,
                                   c2=0.005,
                                   all_possible_transitions=True,
                                   keep_tempfiles=True,
                                   model_filename=tkns.MODEL_FILE,
                                   verbose=True)
    else:
        crf = sklearn_crfsuite.CRF(algorithm='ap',
                                   max_iterations=5000,
                                   epsilon=1e-4,
                                   keep_tempfiles=True,
                                   model_filename=tkns.MODEL_FILE,
                                   verbose=True)

    crf.fit(X_train, y_train)
    print('Training Info:', crf.training_log_.last_iteration)

    # store labels
    labels = list(crf.classes_)

    print('Predicting holdout data...')
    y_pred = crf.predict(X_test)

    print('Performance:')
    total = metrics.flat_f1_score(y_test, y_pred, average='weighted', labels=labels)
    sequence_accuracy = metric.sequence_accuracy_score(y_test, y_pred)
    print('F1-score', total)
    print('Sequence accuracy', sequence_accuracy)

    sorted_labels = sorted(labels, key=lambda name: (name[1:], name[0]))
    print(metrics.flat_classification_report(y_test, y_pred, labels=sorted_labels, digits=3))
    print('Training a model finished')


if __name__ == '__main__':
    X_train, y_train, X_test, y_test = read_data()
    train_new_model(X_train, y_train, X_test, y_test)
