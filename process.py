import pandas as pd

#
#==================================================================================================================
#               FULL PROCESSING
#==================================================================================================================
#

def processFullPOs(full_df: pd.DataFrame):
    full_df = full_df.reset_index(names='id')
    full_df['delta'] = -1

    accrual = full_df.groupby(['poNumber', 'costCenter', 'primaryInternalOrder',
    'profitCenter', 'generalLedgerAccount', 'needByDate',
    'poEndDate', 'poStartDate', 'receivableIndicator',
    'projectWbs', 'matOrSrc'], dropna=False)

    full_df['id'] = accrual['id'].transform('min')

    accrued_df = accrual.agg({
    'id': 'min',
    'delta': 'max',

    'poNumber': 'first',
    'costCenter': 'first',
    'primaryInternalOrder': 'first',
    'profitCenter': 'first',
    'generalLedgerAccount': 'first',
    'needByDate': 'first',
    'poEndDate': 'first',
    'poStartDate': 'first',
    'receivableIndicator': 'first',
    'projectWbs': 'first',
    'matOrSrc': 'first',

    'poName': 'first',
    'poRequisitionerWwid': 'first',
    'poRequisitionerWwid': 'first',
    'poRequisitionerName': 'first',
    'poPreparerWwid': 'first',
    'poPreparerName': 'first',
    'costCenterDesc': 'first',
    'generalLedgerAccountDesc': 'first',
    'projectWbs': 'first',
    'supplierNumber': 'first',
    'supplierName': 'first',
    'supplierEmailAddress': 'first',
    'poType': 'first',
    'poStatus': 'first',
    'poCloseStatus': 'first',
    'poCreationDate': 'first',
    'receiptDates': 'first',
    'invoiceDates': 'first',
    'invoicePaidStatus': 'first',
    'transactionDate': 'first',
    'clearingDocumentRef': 'first',
    'clearingDateReference': 'first',
    'localCurrencyForPoValue': 'first',
    'documentCurrencyForPoValue': 'first',
    'localCurrencyForGoodsReceipt': 'first',
    'documentCurrencyForGoodsReceipt': 'first',
    'localCurrencyForInvoiceReceipt': 'first',
    'docCurrencyForInvoiceReceipt': 'first',
    'poValueInGlobalCurrency': 'first',
    'poValueInLocalCurrency': 'first',
    'poValueInDocCurrency': 'first',
    'gdsReceiptValueInGlobalCurrency': 'first',
    'gdsReceiptValueInlocalCurrency': 'first',
    'gdsReceiptValueInDocCurrency': 'first',
    'invoiceReceiptValueInGlobalCurrency': 'first',
    'invoiceReceiptValueInLocalCurrency': 'first',
    'invoiceReceiptValueInDocCurrency': 'first',
    'aribaBu': 'first',
    'mrc': 'first',
    'companyCode': 'first',
    'legalEntity': 'first',
    'fsid': 'first',
    'region': 'first',
    'businessArea': 'first',
    'shipTo': 'first',
    'deliverTo': 'first',
    'commodityType': 'first',
    'excludeDownpaymentRequestsForPayments': 'first',
    'sourceSystemApprovableId': 'first',
    'requisitionNumber': 'first',
    'receivableIndicator': 'first',
    'poLineNumber': 'first',
    'splitLineNumber': 'first',

    'poValueInGlobalCurrency': 'sum',
    'poValueInLocalCurrency': 'sum',
    'poValueInDocCurrency': 'sum',
    'gdsReceiptValueInGlobalCurrency': 'sum',
    'gdsReceiptValueInlocalCurrency': 'sum',
    'gdsReceiptValueInDocCurrency': 'sum',
    'invoiceReceiptValueInGlobalCurrency': 'sum',
    'invoiceReceiptValueInLocalCurrency': 'sum',
    'invoiceReceiptValueInDocCurrency': 'sum',
    'deliverTo': 'sum'
    }).reset_index(drop=True)

    accrued_df['delta'] = accrued_df['delta'] + 1

    final_df = pd.concat([full_df, accrued_df])

    #Sinaliza que todas POs são válidas, nenhuma sofreu edição ainda
    final_df['isValid'] = True
    
    #Salva id único da PO
    final_df['poId'] = final_df['poId'] = final_df['poNumber'] + final_df['poLineNumber'] + final_df['splitLineNumber']

    final_df = final_df.reset_index(drop=True)
    return final_df

#
#==================================================================================================================
#               DELTA PROCESSING
#==================================================================================================================
#

def processDeltaPOs(delta_raw_df: pd.DataFrame, output_df: pd.DataFrame):
    #Agrupa POs com mesmo identificador único
    accrual = delta_raw_df.groupby(['poNumber', 'poLineNumber', 'splitLineNumber'], dropna=False)

    #Conta quantas vezes a PO se repete para ver se é nova ou editada
    delta_raw_df['count'] = accrual['poId'].transform('count')

    new_po_df = getNewPO(delta_raw_df)
    edited_po_df = getEditPO(delta_raw_df)

    #POs EDITADAS=================================================================================================
    #POs to be edited
    to_edit_po_df = delta_raw_df.loc[(delta_raw_df['count'] == 2) & (delta_raw_df['delta'] == -1)].copy(deep=True)
    del to_edit_po_df['count']

    #Ordena por poId para que 
    edited_po_df.sort_values(by=['poId'], inplace=True)
    to_edit_po_df.sort_values(by=['poId'], inplace=True)

    edited_po_df.reset_index(drop=True, inplace=True)
    edited_po_df['id'] = to_edit_po_df['id'].reset_index(drop=True)

    output_df.loc[(output_df['poId'].isin(to_edit_po_df['poId'])) & (output_df['delta'] == -1),'isValid'] = False
    edited_po_df['isValid'] = True
    output_df = pd.concat([output_df, edited_po_df])
    output_df.reset_index(drop=True)

    #POs NOVAS=================================================================================================
    #POs que a serem processada
    #No dataframe de accruedData está contido as POs editadas, selecionadas por isValid = True
    to_process_df = output_df.loc[(output_df['delta'] == -1) & (output_df['isValid'] == True)].copy()

    #Flag temporária para separar as POs novas das demais
    to_process_df['isNew'] = False
    new_po_df['isNew'] = True

    #POs que já existiam concatenadas com POs novas
    to_accrual_df = pd.concat([to_process_df, new_po_df])

    #Agrupando as POs
    new_accrual = to_accrual_df.groupby(['poNumber', 'costCenter', 'primaryInternalOrder',
        'profitCenter', 'generalLedgerAccount', 'needByDate',
        'poEndDate', 'poStartDate', 'receivableIndicator',
        'projectWbs', 'matOrSrc'], dropna=False)

    #Salva o menor id dos grupos nas novas POs
    to_accrual_df['id'] = new_accrual['id'].transform('min')
    new_po_df = to_accrual_df.loc[to_accrual_df['isNew'] == True].copy(deep=True)

    del to_process_df['isNew']
    del new_po_df['isNew']
    del to_accrual_df['isNew']

    new_accrued_df = new_accrual.agg({
    'id': 'min',
    'delta': 'max',
    'isValid': 'first',

    'poNumber': 'first',
    'costCenter': 'first',
    'primaryInternalOrder': 'first',
    'profitCenter': 'first',
    'generalLedgerAccount': 'first',
    'needByDate': 'first',
    'poEndDate': 'first',
    'poStartDate': 'first',
    'receivableIndicator': 'first',
    'projectWbs': 'first',
    'matOrSrc': 'first',

    'poName': 'first',
    'poRequisitionerWwid': 'first',
    'poRequisitionerWwid': 'first',
    'poRequisitionerName': 'first',
    'poPreparerWwid': 'first',
    'poPreparerName': 'first',
    'costCenterDesc': 'first',
    'generalLedgerAccountDesc': 'first',
    'projectWbs': 'first',
    'supplierNumber': 'first',
    'supplierName': 'first',
    'supplierEmailAddress': 'first',
    'poType': 'first',
    'poStatus': 'first',
    'poCloseStatus': 'first',
    'poCreationDate': 'first',
    'receiptDates': 'first',
    'invoiceDates': 'first',
    'invoicePaidStatus': 'first',
    'transactionDate': 'first',
    'clearingDocumentRef': 'first',
    'clearingDateReference': 'first',
    'localCurrencyForPoValue': 'first',
    'documentCurrencyForPoValue': 'first',
    'localCurrencyForGoodsReceipt': 'first',
    'documentCurrencyForGoodsReceipt': 'first',
    'localCurrencyForInvoiceReceipt': 'first',
    'docCurrencyForInvoiceReceipt': 'first',
    'poValueInGlobalCurrency': 'first',
    'poValueInLocalCurrency': 'first',
    'poValueInDocCurrency': 'first',
    'gdsReceiptValueInGlobalCurrency': 'first',
    'gdsReceiptValueInlocalCurrency': 'first',
    'gdsReceiptValueInDocCurrency': 'first',
    'invoiceReceiptValueInGlobalCurrency': 'first',
    'invoiceReceiptValueInLocalCurrency': 'first',
    'invoiceReceiptValueInDocCurrency': 'first',
    'aribaBu': 'first',
    'mrc': 'first',
    'companyCode': 'first',
    'legalEntity': 'first',
    'fsid': 'first',
    'region': 'first',
    'businessArea': 'first',
    'shipTo': 'first',
    'deliverTo': 'first',
    'commodityType': 'first',
    'excludeDownpaymentRequestsForPayments': 'first',
    'sourceSystemApprovableId': 'first',
    'requisitionNumber': 'first',
    'receivableIndicator': 'first',
    'poLineNumber': 'first',
    'splitLineNumber': 'first',
    
    'poValueInGlobalCurrency': 'sum',
    'poValueInLocalCurrency': 'sum',
    'poValueInDocCurrency': 'sum',
    'gdsReceiptValueInGlobalCurrency': 'sum',
    'gdsReceiptValueInlocalCurrency': 'sum',
    'gdsReceiptValueInDocCurrency': 'sum',
    'invoiceReceiptValueInGlobalCurrency': 'sum',
    'invoiceReceiptValueInLocalCurrency': 'sum',
    'invoiceReceiptValueInDocCurrency': 'sum',
    'deliverTo': 'sum'
    })

    #Incrementa o delta
    new_accrued_df['delta'] = output_df['delta'].max() + 1

    #Dataframe final, adiciona no arquivo de output as POs novas e o novo resultado de processamento
    final_df = pd.concat([output_df, new_po_df, new_accrued_df])
    final_df = final_df.reset_index(drop=True)

    return final_df

#==================================================================================================================
#               AUX FUNCTIONS
#==================================================================================================================

def getEditPO(df: pd.DataFrame):
    #Seleciona POs editadas
    edited_po_df = df.loc[(df['count'] == 2) & (df['delta'] == -2)].copy(deep=True)
    edited_po_df['delta'] = -1
    del edited_po_df['count']
    return edited_po_df

def getNewPO(df: pd.DataFrame):
    #Seleciona POs novas
    new_po_df = df.loc[(df['count'] == 1) & (df['delta'] == -2)].copy(deep=True)
    new_po_df['delta'] = -1
    new_po_df['isValid'] = True
    del new_po_df['poId']
    del new_po_df['count']
    return new_po_df