"""
ONS Address Index - Plot the Performance of the Probabilistic Parser
====================================================================

A simple script to visualise the performance of the probabilistic parser.


Requirements
------------

:requires: pandas
:requires: seaborn
:requires: matplotlib


Author
------

:author: Sami Niemi (sami.niemi@valtech.co.uk)


Version
-------

:version: 0.1
:date: 18-Oct-2016
"""
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

# set seaborn style
sns.set_style("whitegrid")
sns.set_context("poster")
sns.set(rc={"figure.figsize": (12, 12)})
sns.set(font_scale=1.5)


def impactOfTrainingData(outpath='/Users/saminiemi/Projects/ONS/AddressIndex/figs/'):
    """

    :param outpath:

    :return: None
    """
    # load data and use only those with standard parameters
    df = pd.read_csv('/Users/saminiemi/Projects/ONS/AddressIndex/data/parsingResults.csv')
    msk = df['params'] == 'standard'
    df = df.loc[msk]

    # performance figure
    plt.title('Impact of Training Data')
    plt.plot(df['nsamples'], df['allcorrectfrac'], 'gs-', label='Correct Sequences')
    plt.plot(df['nsamples'], df['tokenscorrectfrac'], 'mo-', label='Correct Tokens')
    plt.legend(shadow=True, fancybox=True, loc='lower right')
    plt.xlabel('Number of Training Samples')
    plt.ylabel('Fraction of Sample')
    plt.xlim(1, df['nsamples'].max()*1.05)
    plt.tight_layout()
    plt.savefig(outpath + 'trainingSampleSize.pdf')
    plt.close()

    plt.plot(df['nsamples'], df['loss'], 'gs')
    plt.xlabel('Number of Training Samples')
    plt.ylabel('Loss')
    plt.xlim(1, df['nsamples'].max()*1.05)
    plt.tight_layout()
    plt.savefig(outpath + 'trainingSampleLoss.pdf')
    plt.close()

    plt.plot(df['nsamples'], df['nfeatures'], 'gs')
    plt.xlim(1, df['nsamples'].max()*1.05)
    plt.xlabel('Number of Training Samples')
    plt.ylabel('Number of Features')
    plt.tight_layout()
    plt.savefig(outpath + 'trainingSampleFeatures.pdf')
    plt.close()


if __name__ == "__main__":
    impactOfTrainingData()
