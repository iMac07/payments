package org.xersys.payment.base;

import com.mysql.jdbc.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import javax.sql.rowset.CachedRowSet;
import javax.sql.rowset.RowSetFactory;
import javax.sql.rowset.RowSetProvider;
import org.xersys.commander.contants.EditMode;
import org.xersys.commander.contants.TransactionStatus;
import org.xersys.commander.iface.LRecordMas;
import org.xersys.commander.iface.XNautilus;
import org.xersys.commander.iface.XPaymentInfo;
import org.xersys.commander.iface.XPayments;
import org.xersys.commander.util.MiscUtil;
import org.xersys.commander.util.SQLUtil;

public class ChargeInvoice implements XPayments{
    private final String SOURCE_CODE = "CI";
    
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
    
    public ChargeInvoice(XNautilus foNautilus, String fsBranchCd, boolean fbWithParent){
        p_oNautilus = foNautilus;
        p_sBranchCd = fsBranchCd;
        p_bWithParent = fbWithParent;
        
        p_sSourceCd = "";
        p_sSourceNo = "";
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
        
        try {
            RowSetFactory factory = RowSetProvider.newFactory();

            //create empty master record
            String lsSQL = MiscUtil.addCondition(getSQ_Master(), "0=1");
            ResultSet loRS = p_oNautilus.executeQuery(lsSQL);
            p_oMaster = factory.createCachedRowSet();
            p_oMaster.populate(loRS);
            MiscUtil.close(loRS);
        } catch (SQLException ex) {
            ex.printStackTrace();
            setMessage("SQL Exception on " + lsProcName); 
            p_nEditMode = EditMode.UNKNOWN;
            return false;
        }

        p_nEditMode = EditMode.ADDNEW;

        return true;
    }

    @Override
    public boolean SaveTransaction() {
        System.out.println(this.getClass().getSimpleName() + ".SaveRecord()");
        
        if (p_nEditMode != EditMode.ADDNEW &&
            p_nEditMode != EditMode.UPDATE){
            setMessage("Transaction is not on update mode.");
            return false;
        }       
        
        String lsSQL = "";
        try {            
            if (!p_bWithParent) p_oNautilus.beginTrans();
        
            if ("".equals((String) getMaster("sTransNox"))){ //new record
                Connection loConn = getConnection();

                p_oMaster.updateObject("sTransNox", MiscUtil.getNextCode("Sales_Invoice", "sTransNox", true, loConn, p_sBranchCd));
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
                
                lsSQL = MiscUtil.rowset2SQL(p_oMaster, "Charge_Invoice", "sClientNm");
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
        try {
            String lsSQL = "UPDATE Charge_Invoice SET" +
                                "  cTranStat = '1'" +
                                ", dModified = " + SQLUtil.toSQL(p_oNautilus.getServerDate()) +
                            " WHERE sTransNox = " + SQLUtil.toSQL((String) p_oMaster.getObject("sTransNox"));
            
            if(p_oNautilus.executeUpdate(lsSQL, "Charge_Invoice", p_sBranchCd, "") <= 0){
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
    public boolean PrintTransaction() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
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

            p_oMaster.first();
            p_oMaster.updateObject(fsFieldNm, foValue);
            p_oMaster.updateRow();

            p_oListener.MasterRetreive(fsFieldNm, p_oMaster.getObject(fsFieldNm));
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
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void setCardInfo(XPaymentInfo foValue) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public XPaymentInfo getChequeInfo() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void setChequeInfo(XPaymentInfo foValue) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public XPaymentInfo getGCInfo() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void setGCInfo(XPaymentInfo foValue) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public String getMessage() {
        return p_sMessagex;
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
}
