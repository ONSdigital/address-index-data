"""
ONS Address Index - Optimise the Probabilistic Parser
=====================================================

A simple script to train a CRF model.


Requirements
------------

:requires: sklearn-crfsuite (http://sklearn-crfsuite.readthedocs.io/en/latest/index.html)


Author
------

:author: Sami Niemi (sami.niemi@valtech.co.uk)


Version
-------

:version: 0.1
:date: 20-Oct-2016
"""
import ProbabilisticParser.common.tokens as t
import sklearn_crfsuite
from sklearn_crfsuite import metrics


def readData(trainingfile='/Users/saminiemi/Projects/ONS/AddressIndex/data/training/training1000000.xml',
             holdoutfile='/Users/saminiemi/Projects/ONS/AddressIndex/data/training/holdout.xml',
             verbose=True):
    """
    Read in the training and holdout data from XML files.

    :param trainingfile: location of the training data
    :type trainingfile: str
    :param holdoutfile: location of the holdout data
    :type holdoutfile: str
    :param verbose: whether or not to print to stdout
    :type verbose: bool

    :return: training data and labels, holdout data and labels
    :rtype: list
    """
    if verbose:
        print('Read in training data...')
    X_train, y_train = t.readData(trainingfile)

    if verbose:
        print('Read in holdout data')
    X_test, y_test = t.readData(holdoutfile)


    return X_train, y_train, X_test, y_test


def trainModel(X_train, y_train, X_test, y_test):
    """
    # refer to http://www.chokkan.org/software/crfsuite/manual.html
    # for description of parameters

    :return:
    """
    print('Training a CRF model')
    crf = sklearn_crfsuite.CRF(algorithm='lbfgs',
                               c1=0.4,
                               c2=0.05,
                               all_possible_transitions=True,
                               keep_tempfiles=True,
                               model_filename=t.MODEL_FILE,
                               verbose=True)
    crf.fit(X_train, y_train)
    print('Training Info:', crf.training_log_.last_iteration)

    #store labels
    labels = list(crf.classes_)

    print('Predicting holdout data...')
    y_pred = crf.predict(X_test)

    print('Performance:')
    total = metrics.flat_f1_score(y_test, y_pred, average='weighted', labels=labels)
    print(total)

    sorted_labels = sorted(labels, key=lambda name: (name[1:], name[0]))
    print(metrics.flat_classification_report(y_test, y_pred, labels=sorted_labels, digits=3))
    print('Training a model finished')


if __name__ == '__main__':
    X_train, y_train, X_test, y_test = readData()
    trainModel(X_train, y_train, X_test, y_test)
