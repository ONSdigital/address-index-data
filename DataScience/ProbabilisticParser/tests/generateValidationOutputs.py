"""
ONS Address Index - Validation Outputs for Probabilistic Parser
===============================================================

A simple script to output a generated CRF file features and transitions
to csv files for validation purposes.


Requirements
------------

:requires: ProbabilisticParser (a CRF model specifically build for ONS)
:requires: sklearn-crfsuite (http://sklearn-crfsuite.readthedocs.io/en/latest/index.html)


Author
------

:author: Sami Niemi (sami.niemi@valtech.co.uk)


Version
-------

:version: 0.1
:date: 3-Nov-2016
"""
import ProbabilisticParser.common.tokens as parserTokens
import sklearn_crfsuite
from collections import Counter


def loadCRFmodel():
    """
    Load in a trained CRF model from a file.

    :return: CRF model object as defined by sklearn_crfsuite
    """
    crf = sklearn_crfsuite.CRF(model_filename=parserTokens.MODEL_PATH + parserTokens.MODEL_FILE, verbose=True)
    return crf


def writeTransitions(transFeatures, outfile='/Users/saminiemi/Projects/ONS/AddressIndex/data/ParserTransitions.csv'):
    """
    Outputs the token transitions and the associated weight to a file.

    :param transFeatures: counter of model instance transition features
    :param outfile: name of the output file
    :type outfile: str

    :return: None
    """
    fh = open(outfile, 'w')
    fh.write('from,to,weight\n')
    for (label_from, label_to), weight in transFeatures:
        fh.write("%s,%s,%f\n" % (label_from, label_to, weight))
    fh.close()


def writeFeatures(stateFeatures, outfile='/Users/saminiemi/Projects/ONS/AddressIndex/data/ParserFeatures.csv'):
    """
    Outputs the features that help to predict a label to a file.

    :param stateSeatures: counter of model instance state features
    :param outfile: name of the output file
    :type outfile: str

    :return: None
    """
    fh = open(outfile, 'w')
    fh.write('label,feature,weight\n')
    for (attr, label), weight in stateFeatures:
        fh.write("%s,%s,%f\n" % (label, attr, weight))
    fh.close()


def writeLabels(crf, outfile='/Users/saminiemi/Projects/ONS/AddressIndex/data/ParserLabels.csv'):
    """
    Write CRF model labels to a file.

    :param crf: CRF model object as defined by sklearn_crfsuite
    :type crf: object
    :param outfile: name of the output file
    :type outfile: str

    :return: None
    """
    fh = open(outfile, 'w')
    fh.write('labels\n')
    for label in list(crf.classes_):
        fh.write(str(label) + '\n')
    fh.close()


def writePredictions(predictions, outfile='/Users/saminiemi/Projects/ONS/AddressIndex/data/ParserPredictions.csv'):
    """
    Write CRF model predictions to a file.

    :param predictions:
    :param outfile: name of the output file
    :type outfile: str

    :return: None
    """
    fh = open(outfile, 'w')
    fh.write('labelSequence\n')
    for prediction in predictions:
        fh.write(str(prediction) + '\n')
    fh.close()


def generateOutputfiles(crf, holdoutfile='/Users/saminiemi/Projects/ONS/AddressIndex/data/training/holdout.xml'):
    """
    Generate output files from a CRF model. These include model specific outputs such as
    features and transitions and also predictions for a holdout data set.

    :param crf: CRF model object as defined by sklearn_crfsuite
    :type crf: object
    :param holdoutfile: location and name of the holdout XML data file
    :type holdoutfile: str

    :return: None
    """
    print('Writing labels to a file...')
    writeLabels(crf)

    print("Writing the CRF model transitions to a file...")
    writeTransitions(Counter(crf.transition_features_).most_common())

    print("Writing the CRF model features to a file...")
    writeFeatures(Counter(crf.state_features_).most_common())

    print('Predicting holdout data...')
    X_test, y_test = parserTokens.readData(holdoutfile)
    y_pred = crf.predict(X_test)
    print('Writing model predictions to a file...')
    writePredictions(y_pred)


if __name__ == "__main__":
    crf = loadCRFmodel()
    generateOutputfiles(crf)
