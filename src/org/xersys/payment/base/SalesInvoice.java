package org.xersys.payment.base;

import com.mysql.jdbc.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.sql.rowset.CachedRowSet;
import javax.sql.rowset.RowSetFactory;
import javax.sql.rowset.RowSetProvider;
import org.json.simple.JSONObject;
import org.xersys.commander.contants.EditMode;
import org.xersys.commander.contants.TransactionStatus;
import org.xersys.commander.iface.LRecordMas;
import org.xersys.commander.iface.XNautilus;
import org.xersys.commander.iface.XPaymentInfo;
import org.xersys.commander.iface.XPayments;
import org.xersys.commander.iface.XSearchRecord;
import org.xersys.commander.util.MiscUtil;
import org.xersys.commander.util.SQLUtil;
import org.xersys.commander.util.StringUtil;

public class SalesInvoice implements XPayments, XSearchRecord{
    private final String SOURCE_CODE = "SI";
    private final double VAT_RATE = 0.12;
    
    private XNautilus p_oNautilus;
    private LRecordMas p_oListener;
    
    private String p_sBranchCd;
    private String p_sMessagex;
    private String p_sSourceNo;
    private String p_sSourceCd;
    
    private int p_nEditMode;
    private boolean p_bWithParent;
    
    private CachedRowSet p_oMaster;
    private XPaymentInfo p_oCard;
    private XPaymentInfo p_oCheque;
    private XPaymentInfo p_oGC;
    
    public SalesInvoice(XNautilus foNautilus, String fsBranchCd, boolean fbWithParent){
        p_oNautilus = foNautilus;
        p_sBranchCd = fsBranchCd;
        p_bWithParent = fbWithParent;
        
        p_sSourceCd = "";
        p_sSourceNo = "";
    }
    
    @Override
    public boolean NewRecord() {
        String lsProcName = this.getClass().getSimpleName() + ".NewRecord()";
        
        System.out.println(lsProcName);
        
        if (p_oNautilus == null){
            p_sMessagex = "Application driver is not set.";
            return false;
        }
        
        if (p_sSourceCd.isEmpty() || p_sSourceNo.isEmpty()) {
            setMessage("Insufficient transaction information.");
            return false;
        }
        
        String lsSQL = getSQ_Source();
        ResultSet loRS1 = p_oNautilus.executeQuery(lsSQL);
        
        if (MiscUtil.RecordCount(loRS1) != 0) {
            try {
                RowSetFactory factory = RowSetProvider.newFactory();

                //create empty master record
                lsSQL = MiscUtil.addCondition(getSQ_Master(), "0=1");
                ResultSet loRS = p_oNautilus.executeQuery(lsSQL);
                p_oMaster = factory.createCachedRowSet();
                p_oMaster.populate(loRS);
                MiscUtil.close(loRS);
                addMasterRow();     
                
                loRS1.first();
                p_oMaster.last();
                p_oMaster.updateObject("sClientID", loRS1.getString("sClientID"));
                p_oMaster.updateObject("sClientNm", loRS1.getString("sClientNm"));                
                p_oMaster.updateObject("nTranTotl", loRS1.getDouble("nTranTotl"));
                p_oMaster.updateObject("nDiscount", loRS1.getDouble("nDiscount"));
                p_oMaster.updateObject("nAddDiscx", loRS1.getDouble("nAddDiscx"));
                p_oMaster.updateObject("nFreightx", loRS1.getDouble("nFreightx"));
                p_oMaster.updateObject("nAmtPaidx", loRS1.getDouble("nAmtPaidx"));                
                p_oMaster.updateObject("cTranStat", TransactionStatus.STATE_OPEN);    
                p_oMaster.updateRow();
                MiscUtil.close(loRS1);
                
                p_oCard = new CreditCardTrans(p_oNautilus, p_sBranchCd, true);
                if (!p_oCard.NewTransaction()){
                    setMessage(p_oCard.getMessage()); 
                    p_nEditMode = EditMode.UNKNOWN;
                    return false;
                }
            } catch (SQLException ex) {
                ex.printStackTrace();
                setMessage("SQL Exception on " + lsProcName); 
                p_nEditMode = EditMode.UNKNOWN;
                return false;
            }

            p_nEditMode = EditMode.ADDNEW;

            return true;
        }
        
        setMessage("No record to open.");
        p_nEditMode = EditMode.UNKNOWN;
        return false;        
    }

    @Override
    public boolean SaveRecord() {
        System.out.println(this.getClass().getSimpleName() + ".SaveRecord()");
        
        if (p_nEditMode != EditMode.ADDNEW &&
            p_nEditMode != EditMode.UPDATE){
            setMessage("Transaction is not on update mode.");
            return false;
        }       
        
        String lsSQL = "";
        try {
            if (computePaymentTotal() <= 0.00){
                setMessage("No payment has been made.");
                return false;
            }
            
            if (!p_bWithParent) p_oNautilus.beginTrans();
        
            if ("".equals((String) getMaster("sTransNox"))){ //new record
                Connection loConn = getConnection();

                p_oMaster.updateObject("sTransNox", MiscUtil.getNextCode("Sales_Invoice", "sTransNox", true, loConn, p_sBranchCd));
                p_oMaster.updateObject("sBranchCd", (String) p_oNautilus.getSysConfig("sBranchCd"));
                p_oMaster.updateObject("dTransact", p_oNautilus.getServerDate());
                p_oMaster.updateObject("sSourceCd", p_sSourceCd);
                p_oMaster.updateObject("sSourceNo", p_sSourceNo);
                p_oMaster.updateObject("dModified", p_oNautilus.getServerDate());
                p_oMaster.updateRow();
                
                if (!p_bWithParent) MiscUtil.close(loConn);          
                
                //save the credit card info
                if (p_oCard.getPaymentTotal() > 0.00){
                    p_oCard.setSourceCd(SOURCE_CODE);
                    p_oCard.setSourceNo((String) p_oMaster.getObject("sTransNox"));
                    
                    if (!p_oCard.SaveTransaction()){
                        if (!p_bWithParent) p_oNautilus.rollbackTrans();
                        setMessage(p_oCard.getMessage());
                        return false;
                    }
                }
                
                
                if (!updateSource()) {
                    if (!p_bWithParent) p_oNautilus.rollbackTrans();
                    return false;
                }
                
                lsSQL = MiscUtil.rowset2SQL(p_oMaster, "Sales_Invoice", "sClientNm;nTranTotl;nDiscount;nAddDiscx;nFreightx;nAmtPaidx");
            } else { //old record
            }
            
            if (lsSQL.equals("")){
                if (!p_bWithParent) p_oNautilus.rollbackTrans();
                
                setMessage("No record to update");
                return false;
            }
            
            if(p_oNautilus.executeUpdate(lsSQL, "Sales_Invoice", p_sBranchCd, "") <= 0){
                if(!p_oNautilus.getMessage().isEmpty())
                    setMessage(p_oNautilus.getMessage());
                else
                    setMessage("No record updated");
            } 
           
            if (!p_bWithParent) {
                if(!p_oNautilus.getMessage().isEmpty())
                    p_oNautilus.rollbackTrans();
                else
                    p_oNautilus.commitTrans();
            }    
        } catch (SQLException ex) {
            if (!p_bWithParent) p_oNautilus.rollbackTrans();
            
            ex.printStackTrace();
            setMessage(ex.getMessage());
            return false;
        }
        
        p_nEditMode = EditMode.READY;
        return true;
    }

    @Override
    public boolean UpdateRecord() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public boolean OpenRecord() {
        System.out.println(this.getClass().getSimpleName() + ".OpenRecord()");
        
        return true;
    }
    
    @Override
    public boolean CloseRecord() {
        try {
            String lsSQL = "UPDATE Sales_Invoice SET" +
                                "  cTranStat = '1'" +
                                ", dModified = " + SQLUtil.toSQL(p_oNautilus.getServerDate()) +
                            " WHERE sTransNox = " + SQLUtil.toSQL((String) p_oMaster.getObject("sTransNox"));
            
            if(p_oNautilus.executeUpdate(lsSQL, "Sales_Invoice", p_sBranchCd, "") <= 0){
                if(!p_oNautilus.getMessage().isEmpty())
                    setMessage(p_oNautilus.getMessage());
                else
                    setMessage("No record updated");

                return false;
            }
            
            p_nEditMode = EditMode.UNKNOWN;
            return true;
        } catch (SQLException ex) {
            ex.printStackTrace();
            setMessage("Unable to update Sales Invoice status.");
        }
        
        return false;
    }

    @Override
    public boolean PrintRecord() {
        try {
            //print invoice here
            if ("0".equals((String) p_oMaster.getObject("cTranStat"))){
                if (!CloseRecord()) return false;
            }
            
            p_nEditMode = EditMode.UNKNOWN;
            return true;
        } catch (SQLException ex) {
            ex.printStackTrace();
            setMessage("Unable to print record.");
            
        }
        
        return false;
    }
    
    @Override
    public void setSourceCd(String fsValue) {
        p_sSourceCd = fsValue;
    }

    @Override
    public void setSourceNo(String fsValue) {
        p_sSourceNo = fsValue;
    }

    @Override
    public void setListener(LRecordMas foValue) {
        p_oListener = foValue;
    }
    
    @Override
    public void setMaster(String fsFieldNm, Object foValue) {
        try {
            if (p_nEditMode != EditMode.ADDNEW &&
                p_nEditMode != EditMode.UPDATE){
                System.err.println("Transaction is not on update mode.");
                return;
            }

            switch (fsFieldNm){
                case "sInvNumbr":
                    p_oMaster.first();

                    if (!StringUtil.isNumeric((String) foValue)){
                        //setMessage("Sales Invoice number must be numeric.");
                        p_oMaster.updateObject(fsFieldNm, "");
                    } else {
                        p_oMaster.updateObject(fsFieldNm, (String) foValue);
                    }

                    p_oMaster.updateRow();

                    p_oListener.MasterRetreive(fsFieldNm, p_oMaster.getObject(fsFieldNm));
                    break;
                case "sClientID":
                    p_oListener.MasterRetreive("sClientNm", "");
                    p_oListener.MasterRetreive("sAddressx", "");
                    p_oListener.MasterRetreive("sTINumber", "");
                    break;
                case "nVATSales":
                case "nVATAmtxx":
                case "nNonVATSl":
                case "nZroVATSl":
                    break;
                case "nCWTAmtxx":
                case "nAdvPaymx":
                case "nCashAmtx":
                    p_oMaster.first();

                    if (!StringUtil.isNumeric(String.valueOf(foValue))){
                        //setMessage("Input value must be numeric.");
                        p_oMaster.updateObject(fsFieldNm, 0.00);
                    } else {
                        p_oMaster.updateObject(fsFieldNm, (double) foValue);
                    }

                    p_oMaster.updateRow();
                    
                    computeTax();
                    p_oListener.MasterRetreive(fsFieldNm, p_oMaster.getObject(fsFieldNm));
                    break;
                default:
                    p_oMaster.first();
                    p_oMaster.updateObject(fsFieldNm, foValue);
                    p_oMaster.updateRow();

                    p_oListener.MasterRetreive(fsFieldNm, p_oMaster.getObject(fsFieldNm));
            }
        } catch (SQLException e) {
            e.printStackTrace();
            setMessage("SQLException when assigning value to master record.");
        }
    }

    @Override
    public Object getMaster(String fsFieldNm) {
        try {
            p_oMaster.first();
            switch (fsFieldNm){
                case "nTranTotl":
                    return computeTotal();
                default:
                    return p_oMaster.getObject(fsFieldNm);
            }
            
        } catch (SQLException e) {
            e.printStackTrace();
            setMessage("SQLException when retreiving value to master record.");
            return null;
        }
    }
    
    @Override
    public XPaymentInfo getCreditCardInfo() {
        return p_oCard;
    }

    @Override
    public void setCardInfo(XPaymentInfo foValue) {
        p_oCard = foValue;
        
        try {
            computeTax();
        } catch (SQLException e) {
             e.printStackTrace();
        }
    }

    @Override
    public XPaymentInfo getChequeInfo() {
        return p_oCheque;
    }

    @Override
    public void setChequeInfo(XPaymentInfo foValue) {
        p_oCheque = foValue;
    }

    @Override
    public XPaymentInfo getGCInfo() {
        return p_oGC;
    }

    @Override
    public void setGCInfo(XPaymentInfo foValue) {
        p_oGC = foValue;
    }

    @Override
    public String getMessage() {
        return p_sMessagex;
    }

    @Override
    public JSONObject Search(Enum foType, String fsValue, String fsKey, String fsFilter, int fnMaxRow, boolean fbExact) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public JSONObject SearchRecord(String fsValue, String fsKey, String fsFilter, int fnMaxRow, boolean fbExact) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
    
    private boolean updateSource() throws SQLException{
        String lsSQL = "";
        ResultSet loRS;
        
        double lnPaymTotl = computePaymentTotal();
        
        switch(p_sSourceCd){
            case "SO":
                lsSQL = "SELECT" +
                            " (nTranTotl - ((nTranTotl * nDiscount / 100) + nAddDiscx) + nFreightx - nAmtPaidx) xPayablex" +
                        " FROM Sales_Master" +
                        " WHERE sTransNox = " + SQLUtil.toSQL(p_sSourceNo);
                
                loRS = p_oNautilus.executeQuery(lsSQL);
                if (loRS.next()){                    
                    lsSQL = "UPDATE Sales_Master SET" +
                            "  nAmtPaidx = nAmtPaidx + " + lnPaymTotl;
                    
                    if (loRS.getDouble("xPayablex") <= lnPaymTotl)
                        lsSQL += ", cTranStat = '2'";
                    else 
                        lsSQL += ", cTranStat = '1'";
                    
                    lsSQL = MiscUtil.addCondition(lsSQL, "sTransNox = " + SQLUtil.toSQL(p_sSourceNo));
                }
                MiscUtil.close(loRS);
                
                if (!lsSQL.isEmpty()){
                    if(p_oNautilus.executeUpdate(lsSQL, "Sales_Master", p_sBranchCd, "") <= 0){
                        if(!p_oNautilus.getMessage().isEmpty())
                            setMessage(p_oNautilus.getMessage());
                        else
                            setMessage("No record updated");
                        
                        return false;
                    }
                    
                    return true;
                }
                break;
        }
        
        setMessage("Unable to update source transaction.");
        return false;
    }
    
    private Connection getConnection(){         
        Connection foConn;
        
        if (p_bWithParent){
            foConn = (Connection) p_oNautilus.getConnection().getConnection();
            
            if (foConn == null) foConn = (Connection) p_oNautilus.doConnect();
        } else 
            foConn = (Connection) p_oNautilus.doConnect();
        
        return foConn;
    }
    
    private void setMessage(String fsValue){
        p_sMessagex = fsValue;
    }
    
    private void addMasterRow() throws SQLException{
        p_oMaster.last();
        p_oMaster.moveToInsertRow();
        
        MiscUtil.initRowSet(p_oMaster);
        
        p_oMaster.insertRow();
        p_oMaster.moveToCurrentRow();
    }
     
    private Number computeTotal() throws SQLException{
        double lnTranTotl = (double) p_oMaster.getObject("nTranTotl");
        double lnDiscount = (double) p_oMaster.getObject("nDiscount");
        double lnAddDiscx = (double) p_oMaster.getObject("nAddDiscx");
        double lnFreightx = (double) p_oMaster.getObject("nFreightx");
        double lnAmtPaidx = (double) p_oMaster.getObject("nAmtPaidx");

        return Math.round((lnTranTotl + lnFreightx - lnAmtPaidx - ((lnTranTotl * lnDiscount / 100) + lnAddDiscx)) * 100.0) / 100.0;
    }
    
    private double computePaymentTotal() throws SQLException{
        return (double) p_oMaster.getObject("nCashAmtx") +
                            p_oCard.getPaymentTotal();
        
//                        p_oCheque.getPaymentTotal() +
//                        p_oGC.getPaymentTotal();
    }
    
    private void computeTax() throws SQLException{
        p_oMaster.first();
        
        double lnCashAmtx = (double) p_oMaster.getObject("nCashAmtx");
        double lnAdvPaymx = (double) p_oMaster.getObject("nAdvPaymx");
        double lnCardPaym = p_oCard.getPaymentTotal();
        
        //todo:
        //  get card total; get check total; get gc total
        //  then add to the transaction total
        
        double lnTranTotl = lnCashAmtx + lnAdvPaymx + lnCardPaym;
        
        double lnNonVATSl = 0.00;
        double lnZroVATSl = 0.00;
        double lnCWTAmtxx = 0.00;
        
        double lnVATSales = lnTranTotl / (1 + VAT_RATE);
        double lnVATAmtxx = lnVATSales * VAT_RATE;
        
        p_oMaster.updateObject("nVATSales", Math.round(lnVATSales * 100.0) / 100.0);
        p_oMaster.updateObject("nVATAmtxx", Math.round(lnVATAmtxx * 100.0) / 100.0);
        p_oMaster.updateObject("nNonVATSl", Math.round(lnNonVATSl * 100.0) / 100.0);
        p_oMaster.updateObject("nZroVATSl", Math.round(lnZroVATSl * 100.0) / 100.0);
        p_oMaster.updateObject("nCWTAmtxx", Math.round(lnCWTAmtxx * 100.0) / 100.0);
        p_oMaster.updateRow();
        
        p_oListener.MasterRetreive("nVATSales", p_oMaster.getObject("nVATSales"));
        p_oListener.MasterRetreive("nVATAmtxx", p_oMaster.getObject("nVATAmtxx"));
        p_oListener.MasterRetreive("nNonVATSl", p_oMaster.getObject("nNonVATSl"));
        p_oListener.MasterRetreive("nZroVATSl", p_oMaster.getObject("nZroVATSl"));
    }
    
    private String getSQ_Master(){
        return "SELECT" +
                    "  a.sTransNox" +
                    ", a.sBranchCd" +
                    ", a.dTransact" +
                    ", a.sInvNumbr" +
                    ", a.sClientID" +
                    ", a.nVATSales" +
                    ", a.nVATAmtxx" +
                    ", a.nNonVATSl" +
                    ", a.nZroVATSl" +
                    ", a.nCWTAmtxx" +
                    ", a.nAdvPaymx" +
                    ", a.nCashAmtx" +
                    ", a.sSourceCd" +
                    ", a.sSourceNo" +
                    ", a.cTranStat" +
                    ", a.dModified" +
                    ", IFNULL(b.sClientNm, '') sClientNm" +
                    ", c.nTranTotl" +
                    ", c.nDiscount" +
                    ", c.nAddDiscx" +
                    ", c.nFreightx" +
                    ", c.nAmtPaidx" +
                " FROM Sales_Invoice a" +
                    " LEFT JOIN Client_Master b ON a.sClientID = b.sClientID" +
                    " LEFT JOIN Sales_Master c ON a.sSourceNo = c.sSourceNo";
    }
    
    private String getSQ_Source(){
        String lsSQL = "";
        
        if (p_sSourceCd.contains("SO")){
            lsSQL = "SELECT" +
                        "  IFNULL(c.sClientNm, '') sClientNm" +
                        ", a.nTranTotl" +
                        ", a.nDiscount" +
                        ", a.nAddDiscx" +
                        ", a.nFreightx" +
                        ", a.nAmtPaidx" +
                        ", b.sSourceCd" +
                        ", a.sTransNox" +
                        ", a.sClientID" +
                    " FROM Sales_Master a" +
                        " LEFT JOIN xxxTempTransactions b" +
                            " ON b.sSourceCd = 'SO'" +
                                " AND a.sTransNox = b.sTransNox" + 
                        " LEFT JOIN Client_Master c" + 
                            " ON a.sSalesman = c.sClientID" +
                    " WHERE a.sTransNox = " + SQLUtil.toSQL(p_sSourceNo);
        }
        
        return lsSQL;
    }
}