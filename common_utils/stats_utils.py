import scipy.stats as st
import numpy as np
import pandas as pd
from operator import mul
from functools import reduce
import hvplot.pandas
import warnings
from holoviews import dim, opts
import os
from scipy.stats import ttest_ind

def cramers_corrected_stat(confusion_matrix):
    """ calculate Cramers V statistic for categorial-categorial association.
        uses correction from Bergsma and Wicher,
        Journal of the Korean Statistical Society 42 (2013): 323-328
    """
    chi2 = st.chi2_contingency(confusion_matrix)[0]
    n = confusion_matrix.sum().sum()
    phi2 = chi2/n
    r,k = confusion_matrix.shape
    phi2corr = max(0, phi2 - ((k-1)*(r-1))/(n-1))
    rcorr = r - ((r-1)**2)/(n-1)
    kcorr = k - ((k-1)**2)/(n-1)
    return np.sqrt(phi2corr / min( (kcorr-1), (rcorr-1)))

def get_phi(pdf_x):
    lst_diag = [pdf_x.iloc[i,i] for i in range(pdf_x.shape[0])]
    lst_nondiag = [pdf_x.iloc[i, [j for j in range(pdf_x.shape[1]) if j != i]].values.tolist() for i in
                   range(pdf_x.shape[0])]
    lst_nondiag = [item for sublist in lst_nondiag for item in sublist]
    lst_margins = [pdf_x.iloc[i,:].values.sum() for i in range(pdf_x.shape[0])] + \
                  [pdf_x.iloc[:, i].values.sum() for i in range(pdf_x.shape[1])]
    prod_diag = reduce(mul, lst_diag, 1)
    prod_nondiag = reduce(mul, lst_nondiag, 1)
    prod_margins = reduce(mul, lst_margins, 1)
    phi = (prod_diag - prod_nondiag) / np.sqrt(prod_margins)
    return phi

def get_contingency_table(pdf_included, lst_metrics, pop_col_name, lst_count_col, dct_labels):
    lst_pop_val = sorted(pdf_included.loc[~pdf_included[pop_col_name].isna()][pop_col_name].astype(int).unique().tolist())
    dct_labels = dict([(val, dct_labels[val]) for val in list(dct_labels.keys()) if (val in lst_pop_val)])
    lst_pop_val = sorted([val for val in lst_pop_val if val in dct_labels])
    lst_labels = [dct_labels[val] for val in lst_pop_val]
    lst_pop_size = [pdf_included.loc[pdf_included[pop_col_name] == pop_val].shape[0] for pop_val in lst_pop_val]
    n_1  = pdf_included.loc[pdf_included[pop_col_name] == 1].shape[0]
    n_0 = pdf_included.loc[pdf_included[pop_col_name] == 0].shape[0]
    lst_crosstab = []
    lst_crosstab_pretty = []
    pdf_included = pdf_included.assign(**dict([(col, pd.to_numeric(pdf_included[col], errors='coerce'))
                                               for col in lst_count_col]))
    pdf_included = pdf_included.assign(**dict([(col, pd.to_numeric(pdf_included[col], errors='ignore'))
                                               for col in lst_metrics]))
    for col in lst_metrics + lst_count_col:
        pdf_temp = pdf_included.copy()[[pop_col_name, col]]
        pdf_temp = pdf_temp.loc[pd.notnull(pdf_temp[col])]
        lst_metric_pop_size = [pdf_temp.loc[pdf_temp[pop_col_name] == pop_val].shape[0] for pop_val in lst_pop_val]
        n_1_metric = pdf_temp.loc[pdf_temp[pop_col_name] == 1].shape[0]
        n_0_metric = pdf_temp.loc[pdf_temp[pop_col_name] == 0].shape[0]
        if col in lst_count_col:
            pdf_temp = pdf_temp.assign(**dict([(col, (pdf_temp[col] > 0).astype(int))]))
        pdf_temp = pdf_temp.sort_values(by=[pop_col_name, col])
        corr = pdf_temp[[pop_col_name] + [col]].corr()
        corr = np.nan if ((corr.shape[1] < 2) or (len(lst_pop_val) > 2)) else corr.iloc[0, 1]
        pdf_crosstab = None
        if pdf_temp[col].nunique() > 1:
            pdf_crosstab = pd.crosstab(pdf_temp[pop_col_name],
                                      pdf_temp[col])
            # phi = get_phi(pdf_crosstab)
            p_val = st.chi2_contingency(pdf_crosstab)[1]
            cramers_v = cramers_corrected_stat(pdf_crosstab)
            pdf_crosstab = pd.DataFrame(pdf_crosstab.T.loc[1, :]).T if (len(set(pdf_crosstab.columns.tolist()).difference(set([0, 1]))) == 0)  else \
                            pd.DataFrame(pdf_crosstab.T)
            pdf_crosstab.index = [col] if (pdf_crosstab.shape[0] <=1) else [col + '_' + str(lev)
                                                                            for lev in pdf_crosstab.index.tolist()]
            pdf_crosstab['p_val'] = p_val

            pdf_crosstab['phi'] = corr if (len(pdf_crosstab.index) == 1) else np.nan
            # pdf_crosstab['phi'] = phi
            pdf_crosstab['cramers_v'] = cramers_v if (len(pdf_crosstab.index) == 1) else np.nan
            pdf_crosstab.index.name = 'metric'
            lst_crosstab.append(pdf_crosstab)
        else:
            pdf_crosstab = pd.DataFrame({'phi': corr}, index=[col])
            pdf_crosstab.index.name = 'metric'
            lst_crosstab.append(pdf_crosstab)

        pdf_crosstab_pretty = pdf_crosstab.rename(columns=dict(zip(lst_pop_val, lst_labels)))
        # {0: false_val_name,
        #                                             1: true_val_name})
        pdf_crosstab_pretty = pdf_crosstab_pretty.assign(**dict([(col, np.nan) for col in lst_labels
                                                                 if col not in pdf_crosstab_pretty.columns]))
        # if false_val_name not in pdf_crosstab_pretty.columns:
        #     pdf_crosstab_pretty[false_val_name] = np.nan
        # if true_val_name not in pdf_crosstab_pretty.columns:
        #     pdf_crosstab_pretty[true_val_name] = np.nan
        pdf_crosstab_pretty = pdf_crosstab_pretty.assign(**dict([(tpl_label_n[0], pdf_crosstab_pretty[tpl_label_n[0]].fillna(0).apply(
                lambda x: '{:0.2f} % (N: {}, denom: {})'.format((x * 100 / tpl_label_n[1]) if (tpl_label_n[1] != 0) else np.nan, int(x),
                                                     tpl_label_n[1]))) for tpl_label_n in zip(lst_labels, lst_metric_pop_size)]))
        # pdf_crosstab_pretty[false_val_name] = \
        #     pdf_crosstab_pretty[false_val_name].fillna(0).apply(
        #         lambda x: '{:0.2f} % (N: {}, denom: {})'.format((x * 100 / n_0_metric) if (n_0_metric != 0) else np.nan, int(x),
        #                                              n_0_metric))
        # pdf_crosstab_pretty[true_val_name] = \
        #     pdf_crosstab_pretty[true_val_name].fillna(0).apply(
        #         lambda x: '{:0.2f} % (N: {}, denom: {})'.format((x * 100 / n_1_metric) if (n_1_metric != 0) else np.nan, str(int(x)),
        #                                                         n_1_metric))
        lst_crosstab_pretty.append(pdf_crosstab_pretty)

    pdf_crosstab = pd.concat(lst_crosstab)
    pdf_crosstab_pretty = pd.concat(lst_crosstab_pretty)

    pdf_crosstab_pretty = pdf_crosstab_pretty.rename(columns=dict([(tpl_label_n[0],
                                                                    tpl_label_n[0] + ' (N: {0})'.format(tpl_label_n[1]))
         for tpl_label_n in zip(lst_labels, lst_pop_size)] +
                                                             [('phi', 'correlation with cohort indicator')]     ))
    # pdf_crosstab_pretty = pdf_crosstab_pretty.rename(columns={false_val_name: false_val_name + ' (N: {0})'.format(n_0),
    #                                             true_val_name: true_val_name + ' (N: {0})'.format(n_1),
    #                                             'phi': 'correlation with cohort indicator'})

    # pdf_crosstab_pretty[false_val_name + ' (N: {0})'.format(n_0)] = \
    #     pdf_crosstab_pretty[false_val_name + ' (N: {0})'.format(n_0)].fillna(0).apply(
    #         lambda x: '{:0.2f} % (N: {})'.format(x * 100 / n_0, int(x)))
    # pdf_crosstab_pretty[true_val_name + ' (N: {0})'.format(n_1)] = \
    #     pdf_crosstab_pretty[true_val_name + ' (N: {0})'.format(n_1)].fillna(0).apply(
    #         lambda x: '{:0.2f} % (N: {})'.format(x * 100 / n_1, str(int(x))))

    return pdf_crosstab, pdf_crosstab_pretty

def get_ttest_table(pdf_included, lst_metrics, pop_col_name, dct_labels):
    lst_pop_val = sorted(
        pdf_included.loc[~pdf_included[pop_col_name].isna()][pop_col_name].astype(int).unique().tolist())
    dct_labels = dict([(val, dct_labels[val]) for val in list(dct_labels.keys()) if (val in lst_pop_val)])
    lst_pop_val = sorted([val for val in lst_pop_val if val in dct_labels])
    lst_labels = [dct_labels[val] for val in lst_pop_val]
    lst_pop_size = [pdf_included.loc[pdf_included[pop_col_name] == pop_val].shape[0] for pop_val in lst_pop_val]
    n_1 = pdf_included.loc[pdf_included[pop_col_name] == 1].shape[0]
    n_0 = pdf_included.loc[pdf_included[pop_col_name] == 0].shape[0]
    lst_crosstab = []
    lst_crosstab_pretty = []
    for col in lst_metrics:
        corr = np.nan
        if len(lst_pop_val) == 2:
            corr = pdf_included[[pop_col_name] + [col]].corr().iloc[0, 1]
        lst_pop_metric_mean = [pdf_included[pdf_included[pop_col_name] == pop_val][col].mean() for pop_val in lst_pop_val]
        lst_pop_metric_std = [pdf_included[pdf_included[pop_col_name] == pop_val][col].std() for pop_val in
                              lst_pop_val]
        lst_pop_metric_sem = [pdf_included[pdf_included[pop_col_name] == pop_val][col].sem() for pop_val in
                              lst_pop_val]

        lst_pop_metric_ci = [st.t.interval(0.95, tpl_val[0] - 1,
                    loc=tpl_val[1], scale=tpl_val[2]) for tpl_val in zip(lst_pop_size,
                                                                  lst_pop_metric_mean,
                                                                  lst_pop_metric_sem)]

        # n1_mean = pdf_included[pdf_included[pop_col_name] == 1][col].mean()
        # n1_std = pdf_included[pdf_included[pop_col_name] == 1][col].std()
        # n1_sem = pdf_included[pdf_included[pop_col_name] == 1][col].sem()
        # n1_ci1, n1_ci2 = st.t.interval(0.95, n_1 - 1,
        #             loc=n1_mean, scale=n1_sem)
        # n0_mean = pdf_included[pdf_included[pop_col_name] == 0][col].mean()
        # n0_std = pdf_included[pdf_included[pop_col_name] == 0][col].std()
        # n0_sem = pdf_included[pdf_included[pop_col_name] == 0][col].sem()
        # n0_ci1, n0_ci2 = st.t.interval(0.95, n_0 - 1,
        #                             loc=n0_mean, scale=n0_sem)
        z = np.nan
        p = np.nan
        try:
            if len(lst_pop_val) == 2:
                z, p = st.ranksums(*[pdf_included.loc[pdf_included[pop_col_name] == pop_val][col].dropna()
                                     for pop_val in lst_pop_val])
            if len(lst_pop_val) > 2:
                z, p = st.kruskal(*[pdf_included.loc[pdf_included[pop_col_name] == pop_val][col].dropna()
                                    for pop_val in lst_pop_val])
        except Exception as ex:
            pass
        # t2, p2 = st.ttest_ind(pdf_included.loc[pdf_included[pop_col_name] == 1][col].dropna(),
        #              pdf_included.loc[pdf_included[pop_col_name] == 0][col].dropna())
        pdf_crosstab = pd.DataFrame(dict([('metric', col)] +
                                         [('pop_{0}_mean'.format(tpl_val[0]),
                                           tpl_val[1]) for tpl_val in zip(lst_pop_val, lst_pop_metric_mean)] +
                                         [('pop_{0}_std'.format(tpl_val[0]),
                                           tpl_val[1]) for tpl_val in zip(lst_pop_val, lst_pop_metric_std)] +
                                         [('pop_{0}_ci'.format(tpl_val[0]),
                                           "({0}, {1})".format(tpl_val[1][0], tpl_val[1][1])) for tpl_val in
                                          zip(lst_pop_val, lst_pop_metric_ci)] +
                                         [('rho', corr),
                                          ('z-statistic', z),
                                          ('p-value', z)]),
                                    index=[0])
        lst_crosstab.append(pdf_crosstab)
        pdf_crosstab_pretty = pd.DataFrame(dict([(tpl_val[0] + ' (N: {0})'.format(tpl_val[1]),
                                                  '{:0.2f} (STD: {:0.2f}) (CI: {:0.2f}, {:0.2f})'.format(tpl_val[2],
                                                                                                         tpl_val[3],
                                                                                                         tpl_val[4][0],
                                                                                                         tpl_val[4][1]))
                                                 for tpl_val in zip(lst_labels,
                                                                    lst_pop_size,
                                                                    lst_pop_metric_mean,
                                                                    lst_pop_metric_std,
                                                                    lst_pop_metric_ci)]+
                                                [#(true_val_name + ' (N: {0})'.format(n_1),
                                                 #  '{:0.2f} (STD: {:0.2f}) (CI: {:0.2f}, {:0.2f})'.format(n1_mean,
                                                 #                                                         n1_std,
                                                 #                                                         n1_ci1,
                                                 #                                                         n1_ci2)),
                                                 # (false_val_name + ' (N: {0})'.format(n_0),
                                                 #  '{:0.2f} (STD: {:0.2f}) (CI: {:0.2f}, {:0.2f})'.format(n0_mean,
                                                 #                                                         n0_std,
                                                 #                                                         n0_ci1,
                                                 #                                                         n0_ci2)),
                                                 ('Z score',
                                                  '{0} (p: {1})'.format(str(z), str(p))),
                                                 ('Correlation with indicator variable',
                                                  corr)
                                                 ]),
                                                index=[col])
        pdf_crosstab_pretty.index.name = 'metric'
        lst_crosstab_pretty.append(pdf_crosstab_pretty)
    pdf_crosstab = pd.concat(lst_crosstab, ignore_index=True)
    pdf_crosstab_pretty = pd.concat(lst_crosstab_pretty)
    return pdf_crosstab, pdf_crosstab_pretty

def get_cont_table_statewise(pdf_included, lst_metrics, pop_col_name, lst_count_metrics, dct_labels, lst_st,
                              state_col_name):
    lst_pdf_t = []
    for st in lst_st + ['Σ All States']:
        pdf_t, pdf_t_pretty = get_contingency_table((pdf_included.loc[pdf_included[state_col_name] == st]
                                               if (st != 'Σ All States') else pdf_included),
                                              lst_metrics, pop_col_name, lst_count_metrics,
                                                    dct_labels)
        pdf_t_pretty = pdf_t_pretty.reset_index()
        lst_labels = [lbl for lbl in list(dct_labels.values()) if
                      (len([col for col in pdf_t_pretty.columns if col.startswith(lbl)]) > 0)]
        lst_pop_size = [[col for col in pdf_t_pretty.columns if col.startswith(lbl)][0] for lbl in lst_labels]
        # true_val_N = [col for col in pdf_t_pretty.columns if col.startswith(true_val_name)][0]
        # false_val_N = [col for col in pdf_t_pretty.columns if col.startswith(false_val_name)][0]
        pdf_t_pretty = pdf_t_pretty.rename(columns=dict(zip(lst_pop_size,
                                                            lst_labels))#{true_val_N: true_val_name,
                                                    # false_val_N: false_val_name
                                                    # }
                                           )
        lst_columns = list(pdf_t_pretty.columns)
        pdf_t_pretty[state_col_name] = st + ' ' + " ".join(lst_pop_size) #true_val_N + ' ' + false_val_N
        pdf_t_pretty = pdf_t_pretty[[state_col_name] + lst_columns]
        lst_pdf_t.append(pdf_t_pretty)

    pdf_all_t = pd.concat(lst_pdf_t, ignore_index=True)
    return pdf_all_t.sort_values(by=['metric', state_col_name]).set_index('metric')


def get_ttest_table_statewise(pdf_included, lst_metrics, pop_col_name, dct_labels, lst_st,
                              state_col_name):
    lst_pdf_t = []
    for st in lst_st + ['Σ All States']:
        pdf_t, pdf_t_pretty = get_ttest_table((pdf_included.loc[pdf_included[state_col_name] == st]
                                               if (st != 'Σ All States') else pdf_included),
                                              lst_metrics, pop_col_name, dct_labels)
        pdf_t_pretty = pdf_t_pretty.reset_index()
        lst_labels = [lbl for lbl in list(dct_labels.values()) if (len([col for col in pdf_t_pretty.columns if col.startswith(lbl)]) > 0)]
        lst_pop_size = [[col for col in pdf_t_pretty.columns if col.startswith(lbl)][0] for lbl in lst_labels]
        # true_val_N = [col for col in pdf_t_pretty.columns if col.startswith(true_val_name)][0]
        # false_val_N = [col for col in pdf_t_pretty.columns if col.startswith(false_val_name)][0]
        pdf_t_pretty = pdf_t_pretty.rename(columns=dict(zip(lst_pop_size,
                                                            lst_labels))
        # {true_val_N: true_val_name,
        #                              false_val_N: false_val_name
        #                              }
                            )
        lst_columns = list(pdf_t_pretty.columns)
        pdf_t_pretty[state_col_name] = st + ' ' + ' '.join(lst_pop_size) #true_val_N + ' ' + false_val_N
        pdf_t_pretty = pdf_t_pretty[[state_col_name] + lst_columns]
        lst_pdf_t.append(pdf_t_pretty)

    pdf_all_t = pd.concat(lst_pdf_t, ignore_index=True)
    return pdf_all_t.sort_values(by=['metric', state_col_name]).set_index('metric')

def color_positive_green(x):
    c1 = 'background-color: #d1cfa1'
    c2 = ''
    cols = x.select_dtypes(np.number).columns
    cols = [col for col in cols if col.lower().startswith(('correlation with'))]
    df1 = pd.DataFrame('', index=x.index, columns=x.columns)
    df1[cols] = np.where(x[cols] > 0, c1, c2)
    return df1

def get_descriptives(pdf, lst_st, lst_col, state_col_name):
    lst_pdf_desc = []
    for st in lst_st + ['Σ All States']:
        pdf_state = pdf.loc[pdf[state_col_name] == st] if (st != 'Σ All States') else pdf
        pdf_desc =pdf_state[lst_col].describe(percentiles=[0.01, .25, .5, .75, 0.95, 0.99]).T.reset_index()
        pdf_desc.rename(columns={'index': 'metric'},
                        inplace=True)
        pdf_desc[state_col_name] = st
        pdf_desc = pdf_desc[[state_col_name, 'metric'] + [col for col in pdf_desc.columns if col not in [state_col_name, 'metric']]]
        lst_pdf_desc.append(pdf_desc)
    pdf_all_desc = pd.concat(lst_pdf_desc, ignore_index=True)
    return pdf_all_desc.sort_values(by=['metric', state_col_name]).set_index('metric')

def get_missingness_stats(df, outputfname):
    df_missing = df.map_partitions(
            lambda pdf: pdf.groupby('STATE_CD').apply(lambda gpdf: pd.DataFrame({'n_benes': gpdf.shape[0],
                                                                                 'n_missing_ruca_codes': gpdf.loc[
                                                                                     pd.to_numeric(gpdf['ruca_code'],
                                                                                                   errors='coerce').isna() |
                                                                                     (pd.to_numeric(gpdf['ruca_code'],
                                                                                                    errors='coerce') == -80)].shape[
                                                                                     0],
                                                                                 'n_unmatched_zip_codes': gpdf.loc[
                                                                                     gpdf['out_of_state'].isna()].shape[
                                                                                     0],
                                                                                 'n_out_of_state_benes': gpdf.loc[
                                                                                     gpdf['out_of_state'] == 1].shape[
                                                                                     0],
                                                                                 'n_missing_distance_to_fqhc': gpdf.loc[
                                                                                     ~(pd.to_numeric(
                                                                                         gpdf['distance_to_fqhc'],
                                                                                         errors='coerce') >= 0)].shape[
                                                                                     0],
                                                                                 'n_missing_gender': gpdf.loc[
                                                                                     gpdf['Female'].isna()].shape[0],
                                                                                 'race_hrsa': gpdf.loc[
                                                                                     gpdf['race_hrsa'].isin(
                                                                                         [0, -1])].shape[0],
                                                                                 'n_benes_with_no_diag_codes': gpdf.loc[
                                                                                     gpdf['LST_DIAG_CD'].isna() |
                                                                                     (gpdf[
                                                                                          'LST_DIAG_CD'].str.strip() == '')].shape[
                                                                                     0],
                                                                                 'n_benes_with_no_ndc':
                                                                                     gpdf.loc[gpdf['LST_NDC'].isna() |
                                                                                              (gpdf[
                                                                                                   'LST_NDC'].str.strip() == '')].shape[
                                                                                         0]
                                                                                 }, index=[
                gpdf['STATE_CD'].values[0]]))).compute()
    df_missing = df_missing.reset_index()
    del df_missing['level_1']
    df_missing = df_missing.set_index(['STATE_CD'])
    df_missing = df_missing.groupby('STATE_CD').sum()
    df_missing_with_total = df_missing \
        .reset_index(drop=False) \
        .append(pd.DataFrame({**{'STATE_CD': 'ALL STATES'},
                              **df_missing.sum(axis=0).to_dict()},
                             index=[0])
                )
    df_missing_with_total.to_excel(outputfname, index=False)
    return df_missing_with_total


def get_utilisation_histograms(pdf, lst_covar):
    dct_new_util_vist_names = {'nhrsapcvst': 'No. PC Visits',
                               'nhrsa_nonfqhc_pcvst': 'No. PC Visits (excluding FQHC)',
                               'nhrsaotvst': 'No. OT Visits',
                               'notfqhcvst': 'No. FQHC Visits',
                               'nipvst': 'No. IP Visits',
                               'nedvst': 'No. ED Visits',
                               'nrxvst': 'No. Rx Visits'
                               }
    dct_new_util_cost_names = {'hrsapccost': 'PC Cost',
                               'hrsa_nonfqhc_pccost': 'PC Cost (excluding FQHC)',
                               'hrsaotcost': 'OT Cost',
                               'otfqhccost': 'FQHC Cost',
                               'ipcost': 'IP Cost',
                               'edcost': 'ED Cost',
                               'rxcost': 'Rx Cost'
                               }

    dct_known_titles = {**dct_new_util_vist_names,
                        **dct_new_util_cost_names
                        }
    subplots = ()
    for covar in lst_covar:
        subplots = (subplots + pdf.hvplot.hist(covar, xlabel = dct_known_titles.get(covar, covar),
                                               ylabel='No. Benes', title=dct_known_titles.get(covar, covar)))
    return subplots.opts(opts.Layout(shared_axes=False)).cols(1)

def get_covar_plots(pdf, lst_covar, lst_hist_covar, cut_outliers=False):
    dct_known_titles = {'TotElMon': 'Total Eligible Months',
                        'total_ffs_month': 'Total FFS Months',
                        'total_mc_month': 'Total MC Months',
                        'hrsa_fqhc_ratio': '% of FQHC Visits (in Benes who have FQHC visits)',
                        'elixhauser_score': 'Elixhauser Score',
                        'cdps_mrx_score': 'CDPS MRX Score',
                        'pmca_cond_less': 'PMCA Score',
                        'pmca_cond_more': 'PMCA Score (conservative)'
                        }
    plots = None
    if 'age' in lst_covar:
        age_plots = (pdf.groupby('age').size()
                     .reset_index().rename(columns={0: 'No. Benes'})
                     .hvplot.bar('age', title='Age'))
        if pdf.loc[pdf['age'] < 1].shape[0] > 0:
            age_plots = (age_plots + pdf.loc[pdf['age'] < 1].groupby('age_in_months').size()
                            .reset_index().rename(columns={0: 'No. Benes'})
                            .hvplot.bar('age_in_months', title='Age in months (<1 year old benes)'))


        plots = age_plots if (plots is None) else (plots + age_plots)

    if 'hrsa_fqhc_ratio' in lst_covar:
        fqhc_plots = (pdf.hvplot.hist('hrsa_fqhc_ratio',
                                  ylabel='No. Benes',
                                  title='% of FQHC Visits') +
                        pdf.loc[pdf['hrsa_fqhc_ratio'] > 0].hvplot.hist('hrsa_fqhc_ratio',
                                                                  ylabel='No. Benes',
                                                                  title=dct_known_titles['hrsa_fqhc_ratio'])
                    )
        plots = fqhc_plots if (plots is None) else (plots + fqhc_plots)

    pdf_temp = pdf.copy()
    if cut_outliers:
        pdf_temp = pdf.assign(**dict([(covar,
                                       pdf[covar].where(pdf[covar] <= pdf[covar].quantile(0.99))) for covar in lst_hist_covar]))

    for covar in lst_covar:
        if covar not in ['age', 'hrsa_fqhc_ratio', 'ageday', 'age_in_months']:
            subplot = (pdf_temp.groupby(covar).size()
                   .reset_index().rename(columns={0: 'No. Benes'})
                   .hvplot.bar(covar, title=dct_known_titles.get(covar, covar))
                   if (covar not in lst_hist_covar) else
                   pdf_temp.hvplot.hist(covar, ylabel='No. Benes',
                                   title=dct_known_titles.get(covar, covar)))
            plots = subplot if (plots is None) else (plots + subplot)
    return plots.opts(opts.Layout(shared_axes=False)).cols(1)


def compute_descriptives(pdf, lst_states, lst_metrics, output_fname, state_col_name='STATE_CD'):
    pdf_desc = get_descriptives(pdf,
                                            lst_states,
                                            lst_metrics,
                                            state_col_name)
    pdf_desc.to_excel(output_fname)
    print("Statewise stats is saved at {0}".format(os.path.abspath(output_fname)))
    pdf_desc = pdf_desc.loc[pdf_desc[state_col_name].str.contains('All')]
    del pdf_desc[state_col_name]
    return pdf_desc


def compute_t_stats(pdf, lst_states, lst_metrics, output_fname, pop_col_name='gt_50pc_hrsa_fqhc',
                    dct_labels=None, state_col_name='STATE_CD'):
    if dct_labels is None:
        dct_labels = {0: 'non-FQHC', 1: 'FQHC'}
    pdf_tstats = get_ttest_table_statewise(pdf,
                                                       lst_metrics,
                                                       pop_col_name,
                                                       dct_labels,
                                                       lst_states,
                                                       state_col_name)
    pdf_tstats.to_excel(output_fname)
    print("Statewise T stats is saved at {0}".format(os.path.abspath(output_fname)))
    pdf_tstats = pdf_tstats.loc[pdf_tstats[state_col_name].str.contains('All')]
    print("T stats for {0}".format(pdf_tstats[state_col_name].values[0].replace('Σ', '')))
    del pdf_tstats[state_col_name]
    return pdf_tstats.style.apply(color_positive_green, axis=None)


def compute_contingency_table(pdf, lst_states, lst_metrics, lst_count_metrics, output_fname,
                              pop_col_name='gt_50pc_hrsa_fqhc',
                              dct_labels = None,
                              state_col_name='STATE_CD'):
    if dct_labels is None:
        dct_labels = {0: 'non-FQHC', 1: 'FQHC'}
    pdf_crosstab = get_cont_table_statewise(pdf,
                                            lst_metrics,
                                            pop_col_name,
                                            lst_count_metrics,
                                            dct_labels,
                                            lst_states,
                                            state_col_name)
    pdf_crosstab.to_excel(output_fname)
    print("Statewise contingency table is saved at {0}".format(os.path.abspath(output_fname)))
    pdf_crosstab = pdf_crosstab.loc[pdf_crosstab[state_col_name].str.contains('All')]
    print("Contingency table for {0}".format(pdf_crosstab[state_col_name].values[0].replace('Σ', '')))
    del pdf_crosstab[state_col_name]
    return pdf_crosstab.style.apply(color_positive_green, axis=None)

def compute_missing_stats(df, output_fname, state_col_name = 'STATE_CD'):
    pdf_missing_stats = df.map_partitions(
        lambda pdf: pd.concat([pdf.groupby(state_col_name).agg(**{'n_benes': ('MSIS_ID', 'count')}),
                               pdf.loc[pd.to_numeric(pdf['ruca_code'], errors='coerce').isna() |
                                       (pd.to_numeric(pdf['ruca_code'], errors='coerce') == -80)].groupby(
                                   state_col_name).agg(**{'n_missing_ruca_codes': ('MSIS_ID', 'count')}),
                               pdf.loc[pdf['out_of_state'].isna()].groupby(state_col_name).agg(
                                   **{'n_out_of_state_benes': ('MSIS_ID', 'count')}),
                               pdf.loc[~(pd.to_numeric(pdf['distance_to_fqhc'], errors='coerce') >= 0)].groupby(
                                   state_col_name).agg(**{'n_missing_distance_to_fqhc': ('MSIS_ID', 'count')}),
                               pdf.loc[pdf['Female'].isna()].groupby(state_col_name).agg(
                                   **{'n_missing_gender': ('MSIS_ID', 'count')}),
                               pdf.loc[pdf['race_hrsa'].isin([0, -1])].groupby(state_col_name).agg(
                                   **{'n_missing_race': ('MSIS_ID', 'count')}),
                               pdf.loc[pdf['LST_DIAG_CD'].isna() | (pdf['LST_DIAG_CD'].str.strip() == '')].groupby(
                                   state_col_name).agg(**{'n_benes_with_no_diag_codes': ('MSIS_ID', 'count')}),
                               pdf.loc[pdf['LST_NDC'].isna() | (pdf['LST_NDC'].str.strip() == '')].groupby(
                                   state_col_name).agg(**{'n_benes_with_no_ndc_codes': ('MSIS_ID', 'count')})],
                              axis=1)).compute()
    pdf_missing_stats = pdf_missing_stats.groupby(pdf_missing_stats.index).sum()
    pdf_missing_stats = pdf_missing_stats \
        .reset_index(drop=False).rename(columns={'index': state_col_name}) \
        .append(pd.DataFrame({**{state_col_name: 'All States'},
                              **pdf_missing_stats.sum(axis=0).to_dict()},
                             index=[0])
                ).set_index(state_col_name).astype(int).reset_index(drop=False)
    pdf_missing_stats.to_excel(output_fname)
    print("Statewise missing stats is saved at {0}".format(os.path.abspath(output_fname)))
    pdf_missing_stats = pdf_missing_stats.loc[pdf_missing_stats[state_col_name].str.contains('All')]
    print("Missing stats for {0}".format(pdf_missing_stats[state_col_name].values[0].replace('Σ', '')))
    del pdf_missing_stats[state_col_name]
    return pdf_missing_stats.T


