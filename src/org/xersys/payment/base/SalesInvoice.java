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
import org.xersys.commander.iface.XRecord;
import org.xersys.commander.iface.XSearchRecord;
import org.xersys.commander.util.MiscUtil;
import org.xersys.commander.util.StringUtil;

public class SalesInvoice implements XRecord, XSearchRecord{
    private final String SOURCE_CODE = "SI";
    private final double VAT_RATE = 0.12;
    
    private XNautilus p_oNautilus;
    private LRecordMas p_oListener;
    
    private String p_sBranchCd;
    private String p_sMessagex;
    
    private int p_nEditMode;
    private boolean p_bWithParent;
    
    private CachedRowSet p_oMaster;
    private CachedRowSet p_oCard;
    private CachedRowSet p_oCheck;
    private CachedRowSet p_oGiftCheck;
    
    public SalesInvoice(XNautilus foNautilus, String fsBranchCd, boolean fbWithParent){
        p_oNautilus = foNautilus;
        p_sBranchCd = fsBranchCd;
        p_bWithParent = fbWithParent;
    }
    
    @Override
    public boolean NewRecord() {
        System.out.println(this.getClass().getSimpleName() + ".NewRecord()");
        
        if (p_oNautilus == null){
            p_sMessagex = "Application driver is not set.";
            return false;
        }
        
        try {
            String lsSQL;
            ResultSet loRS;
            
            RowSetFactory factory = RowSetProvider.newFactory();
            
            //create empty master record
            lsSQL = MiscUtil.addCondition(getSQ_Master(), "0=1");
            loRS = p_oNautilus.executeQuery(lsSQL);
            p_oMaster = factory.createCachedRowSet();
            p_oMaster.populate(loRS);
            MiscUtil.close(loRS);
            addMasterRow();            
        } catch (SQLException ex) {
            setMessage(ex.getMessage());
            return false;
        }
        
        p_nEditMode = EditMode.ADDNEW;
        
        return true;
    }

    @Override
    public boolean NewRecord(String fsTmpTrans) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public boolean SaveRecord(boolean fbConfirmed) {
        System.out.println(this.getClass().getSimpleName() + ".SaveRecord()");
        
        if (p_nEditMode != EditMode.ADDNEW &&
            p_nEditMode != EditMode.UPDATE){
            System.err.println("Transaction is not on update mode.");
            return false;
        }
        
        String lsSQL = "";
        
        //if (!isEntryOK()) return false;

        try {
            if (!p_bWithParent) p_oNautilus.beginTrans();
        
            if ("".equals((String) getMaster("sClientID"))){ //new record
                Connection loConn = getConnection();

                p_oMaster.updateObject("sTransNox", MiscUtil.getNextCode("Client_Master", "sClientID", true, loConn, p_sBranchCd));
                p_oMaster.updateObject("sBranchCd", (String) p_oNautilus.getSysConfig("sBranchCd"));
                p_oMaster.updateObject("dModified", p_oNautilus.getServerDate());
                p_oMaster.updateRow();
                
                if (!p_bWithParent) MiscUtil.close(loConn);                        
                
                lsSQL = MiscUtil.rowset2SQL(p_oMaster, "Sales_Invoice", "");
            } else { //old record
            }
            
            if (lsSQL.equals("")){
                if (!p_bWithParent) p_oNautilus.rollbackTrans();
                
                setMessage("No record to update");
                return false;
            }
            
            if(p_oNautilus.executeUpdate(lsSQL, "Client_Master", p_sBranchCd, "") <= 0){
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
        
        p_nEditMode = EditMode.UNKNOWN;
        
        return true;
    }

    @Override
    public boolean UpdateRecord() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public boolean OpenRecord(String fsTransNox) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public boolean DeleteRecord(String fsTransNox) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public boolean DeactivateRecord(String fsTransNox) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public boolean ActivateRecord(String fsTransNox) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public String getMessage() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void setListener(Object foListener) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void setSaveToDisk(boolean fbValue) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Object TempTransactions() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public JSONObject Search(Enum foType, String fsValue, String fsKey, String fsFilter, int fnMaxRow, boolean fbExact) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public JSONObject SearchRecord(String fsValue, String fsKey, String fsFilter, int fnMaxRow, boolean fbExact) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
    
    public Object getMaster(String fsFieldNm) throws SQLException{
        p_oMaster.first();
        return p_oMaster.getObject(fsFieldNm);
    }
    
    public void setMaster(String fsFieldNm, Object foValue) throws Exception, SQLException{
        if (p_nEditMode != EditMode.ADDNEW &&
            p_nEditMode != EditMode.UPDATE){
            System.err.println("Transaction is not on update mode.");
            return;
        }
        
        switch (fsFieldNm){
            case "sInvNumbr":
                p_oMaster.first();
                
                if (!StringUtil.isNumeric((String) foValue)){
                    setMessage("Sales Invoice number must be numeric.");
                        
                    p_oMaster.updateObject(fsFieldNm, "");
                } else {
                    p_oMaster.updateObject(fsFieldNm, (String) foValue);
                }
                
                p_oMaster.updateRow();
                
                p_oListener.MasterRetreive(fsFieldNm, p_oMaster.getObject(fsFieldNm));
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
                    setMessage("Input value must be numeric.");
                        
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
     
    private void computeTax() throws SQLException{
        p_oMaster.first();
        
        double lnCashAmtx = (double) p_oMaster.getObject("nCashAmtx");
        double lnAdvPaymx = (double) p_oMaster.getObject("nAdvPaymx");
        
        //todo:
        //  get card total; get check total; get gc total
        //  then add to the transaction total
        
        double lnTranTotl = lnCashAmtx + lnAdvPaymx;
        
        double lnNonVATSl = 0.00;
        double lnZroVATSl = 0.00;
        double lnCWTAmtxx = 0.00;
        
        double lnVATSales = lnTranTotl / (1 + VAT_RATE);
        double lnVATAmtxx = lnVATSales * VAT_RATE;
        
        p_oMaster.setObject("nVATSales", lnVATSales);
        p_oMaster.setObject("nVATAmtxx", lnVATAmtxx);
        p_oMaster.setObject("nNonVATSl", lnNonVATSl);
        p_oMaster.setObject("nZroVATSl", lnZroVATSl);
        p_oMaster.setObject("nCWTAmtxx", lnCWTAmtxx);
        p_oMaster.updateRow();
    }
    
    private String getSQ_Master(){
        return "SELECT" +
                    "  sTransNox" +
                    ", sBranchCd" +
                    ", dTransact" +
                    ", sInvNumbr" +
                    ", sClientID" +
                    ", nVATSales" +
                    ", nVATAmtxx" +
                    ", nNonVATSl" +
                    ", nZroVATSl" +
                    ", nCWTAmtxx" +
                    ", nAdvPaymx" +
                    ", nCashAmtx" +
                    ", sSourceCd" +
                    ", sSourceNo" +
                    ", dModified" +
                " FROM Sales_Invoice";
    }
}
