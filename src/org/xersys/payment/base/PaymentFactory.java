package org.xersys.payment.base;

import org.xersys.commander.contants.InvoiceType;
import org.xersys.commander.iface.XNautilus;
import org.xersys.commander.iface.XPayments;

public class PaymentFactory {   
    public static XPayments make(String fsValue, XNautilus foNautilus, String fsBranchCd, boolean fbWithParent){
        switch (fsValue){
            case InvoiceType.SALES_INVOICE:
                return new SalesInvoice(foNautilus, fsBranchCd, fbWithParent);
            case InvoiceType.NO_INVOICE:
                return new NoInvoice(foNautilus, fsBranchCd, fbWithParent);
            case InvoiceType.CHARGE_INVOICE:
                return new ChargeInvoice(foNautilus, fsBranchCd, fbWithParent);
            case InvoiceType.OFFICIAL_RECEIPT:
                return new OfficialReceipt(foNautilus, fsBranchCd, fbWithParent);
            default:
                return null;
        }
    }
}