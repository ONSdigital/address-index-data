#!/usr/bin/env python
"""
ONS Address Index - Validation Outputs for Probabilistic Parser
===============================================================

A simple script to output a generated CRF file features and transitions
to csv files for validation purposes.


Running
-------

After all requirements are satisfied, the script can be invoked using CPython interpreter::

    python generateValidationOutputs.py


Requirements
------------

:requires: ProbabilisticParser (a CRF model specifically build for ONS)
:requires: sklearn-crfsuite (http://sklearn-crfsuite.readthedocs.io/en/latest/index.html)


Author
------

:author: Sami Niemi (sami.niemi@valtech.co.uk)


Version
-------

:version: 0.2
:date: 7-Dec-2016
"""
import ProbabilisticParser.common.tokens as parser_tokens
import sklearn_crfsuite
from collections import Counter


def load_CRF_model():
    """
    Load in a trained CRF model from a file.

    :return: CRF model object as defined by sklearn_crfsuite
    """
    crf = sklearn_crfsuite.CRF(model_filename=parser_tokens.MODEL_PATH + parser_tokens.MODEL_FILE, verbose=True)
    return crf


def output_transitions(transition_features,
                       outfile='/Users/saminiemi/Projects/ONS/AddressIndex/data/ParserTransitions.csv'):
    """
    Outputs the token transitions and the associated weight to a file.

    :param transition_features: counter of model instance transition features
    :param outfile: name of the output file
    :type outfile: str

    :return: None
    """
    fh = open(outfile, 'w')
    fh.write('from,to,weight\n')
    for (label_from, label_to), weight in transition_features:
        fh.write("%s,%s,%f\n" % (label_from, label_to, weight))
    fh.close()


def output_features(state_features, outfile='/Users/saminiemi/Projects/ONS/AddressIndex/data/ParserFeatures.csv'):
    """
    Outputs the features that help to predict a label to a file.

    :param state_features: counter of model instance state features
    :param outfile: name of the output file
    :type outfile: str

    :return: None
    """
    fh = open(outfile, 'w')
    fh.write('label,feature,weight\n')
    for (attr, label), weight in state_features:
        fh.write("%s,%s,%f\n" % (label, attr, weight))
    fh.close()


def output_labels(crf, outfile='/Users/saminiemi/Projects/ONS/AddressIndex/data/ParserLabels.csv'):
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


def output_predictions(predictions, outfile='/Users/saminiemi/Projects/ONS/AddressIndex/data/ParserPredictions.csv'):
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


def output_predicted_probabilities(data, crf,
                                   outfile='/Users/saminiemi/Projects/ONS/AddressIndex/data/ParserProbabilities.xml'):
    """
    Output the predicted probabilities to an XML file that contains also the input data.

    :param data: input data containing both the raw input string and components parsed from XML input file
    :param crf: CRF model object as defined by sklearn_crfsuite
    :type crf: object
    :param outfile: name of the output file
    :type outfile: str

    :return: None
    """
    fh = open(outfile, mode='w')
    fh.write('<AddressCollection>\n')

    for raw_string, components in data:
        fh.write('   <AddressString>')

        tokens, labels = list(zip(*components))
        features = parser_tokens.tokens2features(tokens)

        y_pred = crf.predict_single(features)
        y_pred_prob = crf.predict_marginals_single(features)

        for label, token, pred, prob in zip(labels, tokens, y_pred, y_pred_prob):
            prob = round(prob[pred], 6)
            fh.write(
                '<' + label + ' pred_label="' + pred + '" pred_prob="' + str(prob) + '">' + token + "</" + label + ">")

        fh.write('<Input>' + raw_string + '</Input>')
        fh.write('</AddressString>\n')

    fh.write('</AddressCollection>')
    fh.close()


def generate_outputs(crf, holdout_file='/Users/saminiemi/Projects/ONS/AddressIndex/data/training/holdout.xml'):
    """
    Generate output files from a CRF model. These include model specific outputs such as
    features and transitions and also predictions for a holdout data set.

    :param crf: CRF model object as defined by sklearn_crfsuite
    :type crf: object
    :param holdout_file: location and name of the holdout XML data file
    :type holdout_file: str

    :return: None
    """
    print('Writing labels to a file...')
    output_labels(crf)

    print("Writing the CRF model transitions to a file...")
    output_transitions(Counter(crf.transition_features_).most_common())

    print("Writing the CRF model features to a file...")
    output_features(Counter(crf.state_features_).most_common())

    print('Predicting holdout data...')
    X_test, y_test = parser_tokens.readData(holdout_file)
    y_pred = crf.predict(X_test)
    print('Writing model predictions to a file...')
    output_predictions(y_pred)

    print('writing predicted probabilities data to an XML file...')
    data = parser_tokens.readXML(holdout_file)
    output_predicted_probabilities(data, crf)


if __name__ == "__main__":
    crf = load_CRF_model()
    generate_outputs(crf)
