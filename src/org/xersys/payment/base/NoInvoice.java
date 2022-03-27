package org.xersys.payment.base;

import com.mysql.jdbc.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import javax.sql.rowset.CachedRowSet;
import javax.sql.rowset.RowSetFactory;
import javax.sql.rowset.RowSetProvider;
import org.json.simple.JSONObject;
import org.xersys.commander.contants.EditMode;
import org.xersys.commander.iface.LRecordMas;
import org.xersys.commander.iface.XNautilus;
import org.xersys.commander.iface.XPaymentInfo;
import org.xersys.commander.iface.XPayments;
import org.xersys.commander.util.MiscUtil;
import org.xersys.commander.util.SQLUtil;
import org.xersys.commander.util.StringUtil;

public class NoInvoice implements XPayments{    
    private XNautilus p_oNautilus;
    private LRecordMas p_oListener;
    
    private String p_sBranchCd;
    private String p_sMessagex;
    private String p_sSourceNo;
    private String p_sSourceCd;
    private String p_sClientID;
    
    private int p_nEditMode;
    private boolean p_bWithParent;
    
    private CachedRowSet p_oMaster;
    private XPaymentInfo p_oCard;
    private XPaymentInfo p_oCheque;
    private XPaymentInfo p_oGC;
    private double p_nCashAmtx;
    
    public NoInvoice(XNautilus foNautilus, String fsBranchCd, boolean fbWithParent){
        p_oNautilus = foNautilus;
        p_sBranchCd = fsBranchCd;
        p_bWithParent = fbWithParent;
        
        p_sSourceCd = "";
        p_sSourceNo = "";
    }
    
    @Override
    public void setClientID(String fsValue) {
        p_sClientID = fsValue;
    }
    
    @Override
    public boolean NewTransaction() {
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
                p_oMaster.updateObject("nTranTotl", loRS1.getDouble("nTranTotl"));
                p_oMaster.updateObject("nDiscount", loRS1.getDouble("nDiscount"));
                p_oMaster.updateObject("nAddDiscx", loRS1.getDouble("nAddDiscx"));
                p_oMaster.updateObject("nFreightx", loRS1.getDouble("nFreightx"));
                p_oMaster.updateObject("nAmtPaidx", loRS1.getDouble("nAmtPaidx"));                   
                p_oMaster.updateRow();
                MiscUtil.close(loRS1);
                
                
                p_nCashAmtx = 0.00;
                
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
    public boolean SaveTransaction() {
        System.out.println(this.getClass().getSimpleName() + ".SaveRecord()");
        
        if (p_nEditMode != EditMode.ADDNEW){
            setMessage("Transaction is not on update mode.");
            return false;
        }       
        
        try {
            if (computePaymentTotal() <= 0.00){
                setMessage("No payment has been made.");
                return false;
            }
            
            if (!p_bWithParent) p_oNautilus.beginTrans();

            //save the credit card info
            if (p_oCard.getPaymentTotal() > 0.00){
                p_oCard.setSourceCd(p_sSourceCd);
                p_oCard.setSourceNo(p_sSourceNo);

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
            
            if (!p_bWithParent) p_oNautilus.commitTrans();
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
    public boolean UpdateTransaction() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public boolean OpenTransaction(String fsTransNox) {
        System.out.println(this.getClass().getSimpleName() + ".OpenRecord()");
        
        return true;
    }
    
    @Override
    public boolean CloseTransaction() {        
        return true;
    }

    @Override
    public boolean PrintTransaction() {
        return true;
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
        if (p_nEditMode != EditMode.ADDNEW){
            System.err.println("Transaction is not on update mode.");
            return;
        }

        switch (fsFieldNm){
            case "nCashAmtx":
                if (!StringUtil.isNumeric(String.valueOf(foValue))){
                    p_nCashAmtx = 0.00;
                } else {
                    p_nCashAmtx = (double) foValue;
                }
                p_oListener.MasterRetreive(fsFieldNm, p_nCashAmtx);
                break;
        }
    }

    @Override
    public Object getMaster(String fsFieldNm) {
        try {
            switch (fsFieldNm){
                case "nTranTotl":
                    return computeTotal();
                case "nCashAmtx":
                    return p_nCashAmtx;
                default:
                    return null;
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
    
    private boolean updateSource() throws SQLException{
        String lsSQL = "";
        ResultSet loRS;
        
        double lnPaymTotl = computePaymentTotal();
        
        switch(p_sSourceCd){
            case "SO":
                lsSQL = "SELECT" +
                            " (nTranTotl - ((nTranTotl * nDiscount / 100) + nAddDiscx) + nFreightx - nAmtPaidx) xPayablex" +
                        " FROM SP_Sales_Master" +
                        " WHERE sTransNox = " + SQLUtil.toSQL(p_sSourceNo);
                
                loRS = p_oNautilus.executeQuery(lsSQL);
                if (loRS.next()){                    
                    lsSQL = "UPDATE SP_Sales_Master SET" +
                            "  nAmtPaidx = nAmtPaidx + " + lnPaymTotl;
                    
                    if (loRS.getDouble("xPayablex") <= lnPaymTotl)
                        lsSQL += ", cTranStat = '2'";
                    else 
                        lsSQL += ", cTranStat = '1'";
                    
                    lsSQL = MiscUtil.addCondition(lsSQL, "sTransNox = " + SQLUtil.toSQL(p_sSourceNo));
                    
                    MiscUtil.close(loRS);
                    
                    if (!lsSQL.isEmpty()){
                        if(p_oNautilus.executeUpdate(lsSQL, "SP_Sales_Master", p_sBranchCd, "") <= 0){
                            if(!p_oNautilus.getMessage().isEmpty())
                                setMessage(p_oNautilus.getMessage());
                            else
                                setMessage("No record updated");

                            return false;
                        }

                        return true;
                    }
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
        return p_nCashAmtx +
                p_oCard.getPaymentTotal();
    }
    
    private String getSQ_Master(){
        return "SELECT" +
                    "  nTranTotl" +
                    ", nDiscount" +
                    ", nAddDiscx" +
                    ", nFreightx" +
                    ", nAmtPaidx" +
                " FROM SP_Sales_Master";
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
                    " FROM SP_Sales_Master a" +
                        " LEFT JOIN xxxTempTransactions b" +
                            " ON b.sSourceCd = 'SO'" +
                                " AND a.sTransNox = b.sTransNox" + 
                        " LEFT JOIN Client_Master c" + 
                            " ON a.sSalesman = c.sClientID" +
                    " WHERE a.sTransNox = " + SQLUtil.toSQL(p_sSourceNo);
        }
        
        return lsSQL;
    }

    @Override
    public JSONObject searchClient(String fsKey, Object foValue, boolean fbExact) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Object getSearchClient() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
}
