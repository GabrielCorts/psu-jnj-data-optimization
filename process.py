import pandas as pd

#Função para processar as ordens de compra, agrupando-as e somando os valores monetários
#Agrupando Dados
def processPOs(df: pd.DataFrame):
    df = df.reset_index(names='id')
    df['delta'] = -1

    accrual = df.groupby(['poNumber', 'costCenter', 'primaryInternalOrder',
    'profitCenter', 'generalLedgerAccount', 'needByDate',
    'poEndDate', 'poStartDate', 'receivableIndicator',
    'projectWbs', 'matOrSrc'], dropna=False, as_index=False)

    df['id'] = accrual['id'].transform(lambda x: x.min())

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

    final_df = pd.concat([df, accrued_df])
    final_df = final_df.reset_index(drop=True)
    return final_df