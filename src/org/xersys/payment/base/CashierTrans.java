package org.xersys.payment.base;

import java.sql.ResultSet;
import java.sql.SQLException;
import javax.sql.rowset.CachedRowSet;
import javax.sql.rowset.RowSetFactory;
import javax.sql.rowset.RowSetProvider;
import org.xersys.commander.iface.XNautilus;
import org.xersys.commander.util.MiscUtil;
import org.xersys.commander.util.SQLUtil;

public class CashierTrans {
    private XNautilus p_oNautilus;
    
    private String p_sSourceCd;
    private String p_sMessagex;
    
    private CachedRowSet p_oMaster;
    
    public CashierTrans(XNautilus foValue){
        p_oNautilus = foValue;
        p_sSourceCd = "";
    }
    
    public void setSourceCd(String fsValue){
        p_sSourceCd = fsValue;
    }
    
    public int getItemCount() {
        try {
            p_oMaster.last();
            
            return p_oMaster.getRow();
        } catch (SQLException e) {
            e.printStackTrace();
            p_sMessagex = "Unable to get row count of transactions.";
            return -1;
        }
    }
    
    public boolean LoadTransactions() throws SQLException{
        System.out.println(this.getClass().getSimpleName() + ".LoadTransactions()");
        
        p_oMaster = null;
        
        if (p_oNautilus == null){
            p_sMessagex = "Application driver is not set.";
            return false;
        }
        
        if (p_sSourceCd.isEmpty()){
            p_sMessagex = "Transaction Source not defined.";
            return false;
        }
    
        String [] lasSourceCd = p_sSourceCd.split(";");
        
        if (lasSourceCd.length > 1){
            p_sSourceCd = "";
            
            for (int lnCtr = 0; lnCtr <= lasSourceCd.length-1; lnCtr ++){
                p_sSourceCd += ", " + SQLUtil.toSQL(lasSourceCd[lnCtr]);
            }
            
            p_sSourceCd = p_sSourceCd.substring(2);
        }
        
        String lsSQL = getSQ_Master();
        
        ResultSet loRS = p_oNautilus.executeQuery(lsSQL);
        
        if (MiscUtil.RecordCount(loRS) == 0){
            p_sMessagex = "No Transaction to Pay/Release at this time.";
            return false;
        }
        
        RowSetFactory factory = RowSetProvider.newFactory();
        p_oMaster = factory.createCachedRowSet();
        p_oMaster.populate(loRS);
        MiscUtil.close(loRS);
        
        return true;
    }
    
    public Object getDetail(int fnRow, String fsFieldNm) throws SQLException{
        if (fnRow > getItemCount()) return null;
    
        p_oMaster.absolute(fnRow);
        
        switch (fsFieldNm){
            case "nTranTotl":
                return getTranTotal();
            default:
                return p_oMaster.getObject(fsFieldNm);
        }
    }
    
    private double getTranTotal() throws SQLException{
        double lnTranTotl = ((Number) p_oMaster.getObject("nTranTotl")).doubleValue();
        double lnDiscount = ((Number) p_oMaster.getObject("nDiscount")).doubleValue();
        double lnAddDiscx = ((Number) p_oMaster.getObject("nAddDiscx")).doubleValue();
        double lnFreightx = ((Number) p_oMaster.getObject("nFreightx")).doubleValue();
        double lnTotlDisc = (lnTranTotl * (lnDiscount / 100)) + lnAddDiscx;
        
        return lnTranTotl - lnTotlDisc + lnFreightx;
    }
    
    public String getMessage(){
        return p_sMessagex;
    }
    
    private String getSQ_Master(){
        String lsSQL = "";
        
        if (p_sSourceCd.contains("SO")){
            lsSQL = "SELECT" +
                        "  a.dTransact" +
                        ", 'Sales Order' sTranType" +
                        ", IFNULL(CONCAT(b.sSourceCd, ' - ', b.sOrderNox), 'BACKDATE') sOrderNox" +
                        ", a.nTranTotl" +
                        ", a.nDiscount" +
                        ", a.nAddDiscx" +
                        ", a.nFreightx" +
                        ", a.nAmtPaidx" +
                        ", IFNULL(c.sClientNm, '') sClientNm" +
                        ", IFNULL(b.sSourceCd, 'SO') sSourceCd" +
                        ", a.sTransNox" +
                        ", (a.nTranTotl - ((a.nTranTotl * a.nDiscount / 100) + a.nAddDiscx) + a.nFreightx + a.nOthChrge - a.nDeductnx - a.nAmtPaidx) xPayablex" +
                        ", a.nOthChrge" +
                        ", a.nDeductnx" +
                        ", a.cTranStat" +
                    " FROM SP_Sales_Master a" +
                        " LEFT JOIN xxxTempTransactions b" +
                            " ON b.sSourceCd = 'SO'" +
                                " AND a.sTransNox = b.sTransNox" + 
                        " LEFT JOIN Client_Master c" + 
                            " ON a.sSalesman = c.sClientID" +
                    " WHERE a.cTranStat = '0'";
                    //show back date transactions for payment
                    if (!System.getProperty("app.sales.allow.backdate").equals("1")){
                        lsSQL += " AND DATE_FORMAT(a.dTransact, '%Y-%m-%d') = " + SQLUtil.toSQL(SQLUtil.dateFormat(p_oNautilus.getServerDate(), SQLUtil.FORMAT_SHORT_DATE));
                    }            
                    lsSQL += " HAVING xPayablex > 0.00";
            //"  IFNULL(a.dCreatedx, a.dTransact) dTransact" +
            //" AND a.cTranStat IN ('0', '1')" +
            //" HAVING xPayablex >= 0.00"
        }
        
        if (p_sSourceCd.contains("CO")){
            if (!lsSQL.isEmpty()) lsSQL += " UNION ";
            
            lsSQL += "SELECT" +
                        "  a.dTransact" +
                        ", 'Customer Order' sTranType" +
                        ", IFNULL(CONCAT(b.sSourceCd, ' - ', b.sOrderNox), 'BACKDATE') sOrderNox" +
                        ", a.nTranTotl" +
                        ", a.nDiscount" +
                        ", a.nAddDiscx" +
                        ", a.nFreightx" +
                        ", a.nAmtPaidx" +
                        ", '' sClientNm" +
                        ", IFNULL(b.sSourceCd, 'CO') sSourceCd" +
                        ", a.sTransNox" +
                        ", (a.nTranTotl - ((a.nTranTotl * a.nDiscount / 100) + a.nAddDiscx) + a.nFreightx - a.nAmtPaidx) xPayablex" +
                        ", 0.00 nOthChrge" +
                        ", 0.00 nDeductnx" +
                        ", a.cTranStat" +
                    " FROM SP_Sales_Order_Master a" +
                        " LEFT JOIN xxxTempTransactions b" +
                            " ON b.sSourceCd = 'CO'" +
                                " AND a.sTransNox = b.sTransNox" + 
                    " WHERE a.cTranStat = '0'";
                    //show back date transactions for payment
                    if (!System.getProperty("app.sales.allow.backdate").equals("1")){
                        lsSQL += " AND DATE_FORMAT(a.dTransact, '%Y-%m-%d') = " + SQLUtil.toSQL(SQLUtil.dateFormat(p_oNautilus.getServerDate(), SQLUtil.FORMAT_SHORT_DATE));
                    }
                    lsSQL += " HAVING xPayablex > 0.00";
        }
        
        if (p_sSourceCd.contains("WS")){
            if (!lsSQL.isEmpty()) lsSQL += " UNION ";
            
            lsSQL += "SELECT" +
                        "  a.dTransact" +
                        ", 'Whole Sale' sTranType" +
                        ", IFNULL(CONCAT(b.sSourceCd, ' - ', b.sOrderNox), 'BACKDATE') sOrderNox" +
                        ", a.nTranTotl" +
                        ", a.nDiscount" +
                        ", a.nAddDiscx" +
                        ", a.nFreightx" +
                        ", a.nAmtPaidx" +
                        ", 'N-O-N-E' sClientNm" +
                        ", IFNULL(b.sSourceCd, 'WS') sSourceCd" +
                        ", a.sTransNox" +
                        ", (a.nTranTotl - ((a.nTranTotl * a.nDiscount / 100) + a.nAddDiscx) + a.nFreightx - a.nAmtPaidx) xPayablex" +
                        ", 0.00 nOthChrge" +
                        ", 0.00 nDeductnx" +
                        ", a.cTranStat" +
                    " FROM WholeSale_Master a" +
                        " LEFT JOIN xxxTempTransactions b" +
                            " ON b.sSourceCd = 'WS'" +
                                " AND a.sTransNox = b.sTransNox" + 
                    " WHERE a.cTranStat IN ('0', '1')";
                    //show back date transactions for payment
                    if (!System.getProperty("app.sales.allow.backdate").equals("1")){
                        lsSQL += " AND DATE_FORMAT(a.dTransact, '%Y-%m-%d') = " + SQLUtil.toSQL(SQLUtil.dateFormat(p_oNautilus.getServerDate(), SQLUtil.FORMAT_SHORT_DATE));
                    }
                    lsSQL += " HAVING xPayablex > 0.00";
        }
        
        if (p_sSourceCd.contains("JO")){
            if (!lsSQL.isEmpty()) lsSQL += " UNION ";
            
            lsSQL += "SELECT" + 
                        "  a.dTransact" +
                        ", 'Job Order' sTranType" +
                        ", IFNULL(CONCAT(b.sSourceCd, ' - ', b.sOrderNox), 'BACKDATE') sOrderNox" +
                        ", a.nTranTotl" +
                        ", 0.00 nDiscount" +
                        ", 0.00 nAddDiscx" +
                        ", a.nFreightx" +
                        ", a.nAmtPaidx" +
                        ", IFNULL(c.sClientNm, '') sClientNm" +
                        ", IFNULL(b.sSourceCd, 'JO') sSourceCd" +
                        ", a.sTransNox" +
                        ", a.nTranTotl + a.nFreightx - a.nAmtPaidx xPayablex" + 
                        ", 0.00 nOthChrge" +
                        ", 0.00 nDeductnx" +
                        ", a.cTranStat" +
                    " FROM Job_Order_Master a" + 
                        " LEFT JOIN xxxTempTransactions b" + 
                           " ON b.sSourceCd = 'JO'" + 
                           " AND a.sTransNox = b.sTransNox" + 
                        " LEFT JOIN Client_Master c" + 
                           " ON a.sMechanic = c.sClientID";
                    //show back date transactions for payment
                    if (!System.getProperty("app.sales.allow.backdate").equals("1")){
                        lsSQL += " WHERE DATE_FORMAT(a.dTransact, '%Y-%m-%d') = " + SQLUtil.toSQL(SQLUtil.dateFormat(p_oNautilus.getServerDate(), SQLUtil.FORMAT_SHORT_DATE));
                    }
                    lsSQL += " HAVING xPayablex > 0.00 AND a.cTranStat NOT IN ('3', '4')";
                    //" AND a.cTranStat IN ('0', '1')" + 
        }
        
        return lsSQL;
    }
}
