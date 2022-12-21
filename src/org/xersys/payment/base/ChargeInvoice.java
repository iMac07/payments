package org.xersys.payment.base;

import com.mysql.jdbc.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.sql.rowset.CachedRowSet;
import javax.sql.rowset.RowSetFactory;
import javax.sql.rowset.RowSetProvider;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.xersys.clients.search.ClientSearch;
import org.xersys.commander.contants.EditMode;
import org.xersys.commander.contants.TransactionStatus;
import org.xersys.commander.iface.LRecordMas;
import org.xersys.commander.iface.XNautilus;
import org.xersys.commander.iface.XPaymentInfo;
import org.xersys.commander.iface.XPayments;
import org.xersys.commander.util.MiscUtil;
import org.xersys.commander.util.SQLUtil;

public class ChargeInvoice implements XPayments{    
    private final String MASTER_TABLE = "Charge_Invoice";
    
    private final XNautilus p_oNautilus;
    private final boolean p_bWithParent;
    private final String p_sBranchCd;
    
    private LRecordMas p_oListener;
    private String p_sMessagex;
    private String p_sSourceNo;
    private String p_sSourceCd;
    private String p_sClientID;
    
    private int p_nEditMode;
    
    private CachedRowSet p_oMaster;
    
    private final ClientSearch p_oSearchClient;
    
    public ChargeInvoice(XNautilus foNautilus, String fsBranchCd, boolean fbWithParent){
        p_oNautilus = foNautilus;
        p_sBranchCd = fsBranchCd;
        p_bWithParent = fbWithParent;
        
        p_oSearchClient = new ClientSearch(p_oNautilus, ClientSearch.SearchType.searchClient);
        
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
                p_oMaster.updateObject("nAmountxx", loRS1.getDouble("nTranTotl"));                 
                p_oMaster.updateRow();
                
                if (!loRS1.getString("sClientID").equals("")) 
                    getClient("a.sClientID", loRS1.getString("sClientID"));
                
                MiscUtil.close(loRS1);
            } catch (SQLException ex) {
                ex.printStackTrace();
                setMessage("SQL Exception on " + lsProcName); 
                p_nEditMode = EditMode.UNKNOWN;
                return false;
            } catch (ParseException ex) {
                ex.printStackTrace();
                setMessage("Parse Exception on " + lsProcName); 
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
        
        if (p_nEditMode != EditMode.ADDNEW &&
            p_nEditMode != EditMode.UPDATE){
            setMessage("Transaction is not on update mode.");
            return false;
        }       
        
        String lsSQL = "";
        try {           
            p_oMaster.first();
            if (p_oMaster.getString("sClientID").isEmpty()){
                setMessage("Client must not be empty.");
                return false;
            }

            if (!p_bWithParent) p_oNautilus.beginTrans();
        
            if (p_nEditMode == EditMode.ADDNEW){
                Connection loConn = getConnection();

                p_oMaster.updateObject("sTransNox", MiscUtil.getNextCode(MASTER_TABLE, "sTransNox", true, loConn, p_sBranchCd));
                p_oMaster.updateObject("sSourceCd", p_sSourceCd);
                p_oMaster.updateObject("sSourceNo", p_sSourceNo);
                p_oMaster.updateObject("dModified", p_oNautilus.getServerDate());
                p_oMaster.updateRow();
                
                if (!p_bWithParent) MiscUtil.close(loConn);          

                if (!updateSource()) {
                    if (!p_bWithParent) p_oNautilus.rollbackTrans();
                    return false;
                }
                
                lsSQL = MiscUtil.rowset2SQL(p_oMaster, MASTER_TABLE, "sClientNm");
            } else { //old record
                
            }
            
            if (lsSQL.equals("")){
                if (!p_bWithParent) p_oNautilus.rollbackTrans();
                
                setMessage("No record to update");
                return false;
            }
            
            if(p_oNautilus.executeUpdate(lsSQL, MASTER_TABLE, p_sBranchCd, "") <= 0){
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
        if (p_nEditMode != EditMode.READY) return false;
        
        p_nEditMode = EditMode.UPDATE;
        return true;
    }

    @Override
    public boolean OpenTransaction(String fsTransNox) {
        System.out.println(this.getClass().getSimpleName() + ".OpenRecord()");
        
        p_nEditMode = EditMode.READY;
        return true;
    }
    
    @Override
    public boolean CloseTransaction() {        
        try {
            String lsSQL = "UPDATE " + MASTER_TABLE + " SET" +
                                "  cTranStat = '1'" +
                                ", dModified = " + SQLUtil.toSQL(p_oNautilus.getServerDate()) +
                            " WHERE sTransNox = " + SQLUtil.toSQL((String) p_oMaster.getObject("sTransNox"));
            
            if(p_oNautilus.executeUpdate(lsSQL, MASTER_TABLE, p_sBranchCd, "") <= 0){
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

        try {
            switch (fsFieldNm){
                case "sClientID":
                    getClient("a.sClientID", foValue);
                    break;
            }
        } catch (SQLException | ParseException e) {
            e.printStackTrace();
            setMessage("SQLException when assigning value to master record.");
        }
    }

    @Override
    public Object getMaster(String fsFieldNm) {
        try {
            p_oMaster.first();
            switch (fsFieldNm){
                case "sClientID":
                    return "";
                case "nAmountxx":
                    return p_oMaster.getObject(fsFieldNm);
                default:
                    return null;
            }
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return null;
    }

    @Override
    public String getMessage() {
        return p_sMessagex;
    }
    
    private boolean updateSource() throws SQLException{
        String lsSQL;
        ResultSet loRS;
        
        switch(p_sSourceCd){
            case "SO":
                lsSQL = "SELECT" +
                            " (nTranTotl - ((nTranTotl * nDiscount / 100) + nAddDiscx) + nFreightx - nAmtPaidx) xPayablex" +
                        " FROM SP_Sales_Master" +
                        " WHERE sTransNox = " + SQLUtil.toSQL(p_sSourceNo);
                
                loRS = p_oNautilus.executeQuery(lsSQL);
                if (loRS.next()){                    
//                    lsSQL = "UPDATE SP_Sales_Master SET" +
//                            "  nAmtPaidx = nAmtPaidx + " + loRS.getDouble("xPayablex") +
//                            ", sClientID = " + SQLUtil.toSQL(p_oMaster.getString("sClientID"));
                    
                    lsSQL = "UPDATE SP_Sales_Master SET" +
                            "  sClientID = " + SQLUtil.toSQL(p_oMaster.getString("sClientID")) +
                            ", cTranStat = '2'";
                    
//                    if (loRS.getDouble("xPayablex") <= loRS.getDouble("xPayablex"))
//                        lsSQL += ", cTranStat = '2'";
//                    else 
//                        lsSQL += ", cTranStat = '1'";
                    
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
            case "WS":
                lsSQL = "SELECT" +
                            " (nTranTotl - ((nTranTotl * nDiscount / 100) + nAddDiscx) + nFreightx - nAmtPaidx) xPayablex" +
                        " FROM WholeSale_Master" +
                        " WHERE sTransNox = " + SQLUtil.toSQL(p_sSourceNo);
                
                loRS = p_oNautilus.executeQuery(lsSQL);
                if (loRS.next()){                    
//                    lsSQL = "UPDATE WholeSale_Master SET" +
//                            "  nAmtPaidx = nAmtPaidx + " + loRS.getDouble("xPayablex") +
//                            ", sClientID = " + SQLUtil.toSQL(p_oMaster.getString("sClientID"));
                    lsSQL = "UPDATE WholeSale_Master SET" +                            
                            "  sClientID = " + SQLUtil.toSQL(p_oMaster.getString("sClientID")) +
                            ", cTranStat = '2'";
                    
//                    if (loRS.getDouble("xPayablex") <= loRS.getDouble("xPayablex"))
//                        lsSQL += ", cTranStat = '2'";
//                    else 
//                        lsSQL += ", cTranStat = '1'";
                    
                    lsSQL = MiscUtil.addCondition(lsSQL, "sTransNox = " + SQLUtil.toSQL(p_sSourceNo));
                    
                    MiscUtil.close(loRS);
                    
                    if (!lsSQL.isEmpty()){
                        if(p_oNautilus.executeUpdate(lsSQL, "WholeSale_Master", p_sBranchCd, "") <= 0){
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
        p_oMaster.updateObject("cBilledxx", "0");
        p_oMaster.updateObject("cPaidxxxx", "0");
        p_oMaster.updateObject("cWaivexxx", "0");
        p_oMaster.updateObject("cTranStat", TransactionStatus.STATE_OPEN);
        
        p_oMaster.insertRow();
        p_oMaster.moveToCurrentRow();
    }
    
    private String getSQ_Master(){
        return "SELECT" +
                    "  a.sTransNox" +
                    ", a.sClientID" +
                    ", a.cBilledxx" +
                    ", a.dBilledxx" +
                    ", a.cPaidxxxx" +
                    ", a.dPaidxxxx" +
                    ", a.cWaivexxx" +
                    ", a.dWaivexxx" +
                    ", a.sWaivexxx" +
                    ", a.nAmountxx" +	
                    ", a.nAmtPaidx" +	
                    ", a.sSourceCd" +	
                    ", a.sSourceNo" +	
                    ", a.cTranStat" +
                    ", a.dModified" +
                    ", b.sClientNm" +
                " FROM Charge_Invoice a" +
                    " LEFT JOIN Client_Master b ON a.sClientID = b.sClientID";
    }
    
    private String getSQ_Source(){
        String lsSQL = "";
        
        switch (p_sSourceCd){
            case "SO":
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
                            " ON a.sClientID = c.sClientID" +
                    " WHERE a.sTransNox = " + SQLUtil.toSQL(p_sSourceNo);
                break;
            case "WS":
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
                    " FROM WholeSale_Master a" +
                        " LEFT JOIN xxxTempTransactions b" +
                            " ON b.sSourceCd = 'WS'" +
                                " AND a.sTransNox = b.sTransNox" + 
                        " LEFT JOIN Client_Master c" + 
                            " ON a.sClientID = c.sClientID" +
                    " WHERE a.sTransNox = " + SQLUtil.toSQL(p_sSourceNo);
                break;
        }
        
        return lsSQL;
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
    public JSONObject searchClient(String fsKey, Object foValue, boolean fbExact) {
        p_oSearchClient.setKey(fsKey);
        p_oSearchClient.setValue(foValue);
        p_oSearchClient.setExact(fbExact);
        
        return p_oSearchClient.Search();
    }

    @Override
    public Object getSearchClient() {
        return p_oSearchClient;
    }
    
    private void getClient(String fsFieldNm, Object foValue) throws SQLException, ParseException{       
        JSONObject loJSON = searchClient(fsFieldNm, foValue, true);
        JSONParser loParser = new JSONParser();

        if ("success".equals((String) loJSON.get("result"))){
            loJSON = (JSONObject) ((JSONArray) loParser.parse((String) loJSON.get("payload"))).get(0);

            p_oMaster.first();
            p_oMaster.updateObject("sClientID", (String) loJSON.get("sClientID"));
            p_oMaster.updateObject("sClientNm", (String) loJSON.get("sClientNm"));
            p_oMaster.updateRow();            
            
            if (p_oListener != null) p_oListener.MasterRetreive("sClientID", (String) p_oMaster.getObject("sClientNm"));
        }
    }
}
