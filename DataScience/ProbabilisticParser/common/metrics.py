"""
ONS Address Index - Probabilistic Parser Common Metrics
=======================================================

This file defines common metrics that can be used to test the performance of a trained
probabilistic parser model.


Requirements
------------


Author
------

:author: Sami Niemi (sami.niemi@valtech.co.uk)


Version
-------

:version: 0.1
:date: 25-Oct-2016
"""


def sequence_accuracy_score(y_true, y_pred):
    """
    Return sequence accuracy score. Match is counted only when the two sequences are equal.

    :param y_true: true labels
    :param y_pred: predicted labels

    :return: sequence accuracy as a fraction of the sample (1 is prefect, 0 is all incorrect)
    """
    total = len(y_true)

    matches = sum(1 for yseq_true, yseq_pred in zip(y_true, y_pred) if list(yseq_true) == list(yseq_pred))

    return matches / total
