package org.xersys.payment.base;

import com.mysql.jdbc.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import javax.sql.rowset.CachedRowSet;
import javax.sql.rowset.RowSetFactory;
import javax.sql.rowset.RowSetProvider;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.xersys.commander.contants.EditMode;
import org.xersys.commander.contants.TransactionStatus;
import org.xersys.commander.iface.LRecordMas;
import org.xersys.commander.iface.XNautilus;
import org.xersys.commander.iface.XPaymentInfo;
import org.xersys.commander.util.MiscUtil;
import org.xersys.parameters.search.ParamSearchF;

public class CreditCardTrans implements XPaymentInfo{
    private XNautilus p_oNautilus;
    private LRecordMas p_oListener;
    
    private int p_nEditMode;
    private boolean p_bWithParent;
    private String p_sBranchCd;
    private String p_sMessagex;
    
    private CachedRowSet p_oDetail;
    
    private String p_sSourceNo;
    private String p_sSourceCd;
    
    private ParamSearchF p_oBanks;
    
    public CreditCardTrans(){
        p_oNautilus = null;
        p_bWithParent = false;
        
        p_sBranchCd = "";
        p_sMessagex = "";
        p_sSourceCd = "";
        p_sSourceNo = "";
        
        p_nEditMode = EditMode.UNKNOWN;
    }
    
    public CreditCardTrans(XNautilus foValue, String fsBranchCd, boolean fbWithParent){
        p_oNautilus = foValue;
        p_sBranchCd = fsBranchCd;
        p_bWithParent = fbWithParent;
        
        p_sMessagex = "";
        p_sSourceCd = "";
        p_sSourceNo = "";
        
        p_oBanks = new ParamSearchF(p_oNautilus, ParamSearchF.SearchType.searchBanks);
        
        p_nEditMode = EditMode.UNKNOWN;
    }
    
    @Override
    public void setSourceCd(String fsValue){
        p_sSourceCd = fsValue;
    }
    
    @Override
    public void setSourceNo(String fsValue){
        p_sSourceNo = fsValue;
    }
    
    @Override
    public void setListener(LRecordMas foValue) {
        p_oListener = foValue;
    }
    
    @Override
    public String getMessage(){
        return p_sMessagex;
    }
    
    @Override
    public int getItemCount() {
        String lsProcName = this.getClass().getSimpleName() + ".getItemCount()";
        
        try {
            p_oDetail.last();
            return p_oDetail.getRow();
        } catch (SQLException e) {
            e.printStackTrace();
            setMessage("SQLException on " + lsProcName + ". Please inform your System Admin.");
            return -1;
        }
    }
    
    @Override
    public double getPaymentTotal() {
        String lsProcName = this.getClass().getSimpleName() + ".getPaymentTotal()";
        
        double lnTotal = 0.00;
        
        for (int lnCtr = 1; lnCtr <= getItemCount(); lnCtr++){
            try {
                p_oDetail.absolute(lnCtr);
                lnTotal += ((Number) p_oDetail.getObject("nAmountxx")).doubleValue();
            } catch (SQLException ex) {
                ex.printStackTrace();
                setMessage("SQLException on " + lsProcName + ". Please inform your System Admin.");
                return -1;
            }
        }
        
        return lnTotal;
    }
    
    @Override
    public boolean NewTransaction(){
        String lsProcName = this.getClass().getSimpleName() + ".NewTransaction()";
        
        if (p_oNautilus == null){
            p_sMessagex = "Application driver is not set.";
            return false;
        }
        
        try {
            RowSetFactory factory = RowSetProvider.newFactory();

            //create empty master record
            String lsSQL = MiscUtil.addCondition(getSQ_Detail(), "0=1");
            ResultSet loRS  = p_oNautilus.executeQuery(lsSQL);
            p_oDetail = factory.createCachedRowSet();
            p_oDetail.populate(loRS);
            MiscUtil.close(loRS);
            addDetail();     

            p_nEditMode = EditMode.ADDNEW;

            return true;
        } catch (SQLException ex) {
            ex.printStackTrace();
            setMessage("SQLException on " + lsProcName + ". Please inform your System Admin.");
            p_nEditMode = EditMode.UNKNOWN;
            return false;
        }  
    }
    
    @Override
    public boolean SaveTransaction() {
        String lsProcName = this.getClass().getSimpleName() + ".SaveTransaction()";
        
        if (p_nEditMode != EditMode.ADDNEW &&
            p_nEditMode != EditMode.UPDATE){
            System.err.println("Transaction is not on update mode.");
            return false;
        }
        
        String lsSQL = "";

        try {
            if (p_nEditMode == EditMode.ADDNEW){
                if (getItemCount() > 1){
                    //check last record;
                    //if details are compelete, delete the record
                    p_oDetail.last();                
                    if (String.valueOf(p_oDetail.getObject("sTermnlID")).isEmpty() ||
                        String.valueOf(p_oDetail.getObject("sBankCode")).isEmpty() ||
                        String.valueOf(p_oDetail.getObject("sCardNoxx")).isEmpty() ||
                        String.valueOf(p_oDetail.getObject("sApprovNo")).isEmpty() ||
                        ((Number) p_oDetail.getObject("nAmountxx")).doubleValue() <= 0.00){
                        
                        delDetail(getItemCount());
                    }
                }
                
                if (!p_bWithParent) p_oNautilus.beginTrans();
                
                Connection loConn = getConnection();
            
                for (int lnCtr = 1; lnCtr <= getItemCount(); lnCtr++){
                    p_oDetail.absolute(lnCtr);
                    p_oDetail.updateObject("sTransNox", MiscUtil.getNextCode("Credit_Card_Trans", "sTransNox", true, loConn, p_sBranchCd));
                    p_oDetail.updateObject("sBranchCd", p_sBranchCd);
                    p_oDetail.updateObject("sSourceCd", p_sSourceCd);
                    p_oDetail.updateObject("sSourceNo", p_sSourceNo);
                    p_oDetail.updateObject("dModified", p_oNautilus.getServerDate());

                    lsSQL = MiscUtil.rowset2SQL(p_oDetail, "Credit_Card_Trans", "sBankName");
                    
                    if (lsSQL.equals("")){
                        if (!p_bWithParent) p_oNautilus.rollbackTrans();
                        
                        setMessage("No record to save.");
                        return false;
                    }
                    
                    if(p_oNautilus.executeUpdate(lsSQL, "Credit_Card_Trans", p_sBranchCd, "") <= 0){
                        if(!p_oNautilus.getMessage().isEmpty())
                            setMessage(p_oNautilus.getMessage());
                        else{
                            setMessage("No record saved.");
                            break;
                        }
                    }
                }
            }

            if (!p_bWithParent) {
                if(!p_oNautilus.getMessage().isEmpty()){
                    p_oNautilus.rollbackTrans();
                    return false;
                } else p_oNautilus.commitTrans();
            }    
        } catch (SQLException ex) {
            if (!p_bWithParent) p_oNautilus.rollbackTrans();
            
            ex.printStackTrace();
            setMessage("SQLException on " + lsProcName + ". Please inform your System Admin.");
            return false;
        }
        
        p_nEditMode = EditMode.UNKNOWN;
        return true;
    }
    
    @Override
    public boolean OpenTransaction() {
        String lsProcName = this.getClass().getSimpleName() + ".NewTransaction()";
        
        if (p_oNautilus == null){
            p_sMessagex = "Application driver is not set.";
            return false;
        }
        
        try {
            RowSetFactory factory = RowSetProvider.newFactory();

            //create empty master record
            String lsSQL = MiscUtil.addCondition(getSQ_Detail(), "0=1");
            ResultSet loRS  = p_oNautilus.executeQuery(lsSQL);
            p_oDetail = factory.createCachedRowSet();
            p_oDetail.populate(loRS);
            MiscUtil.close(loRS);
            addDetail();     

            p_nEditMode = EditMode.ADDNEW;

            return true;
        } catch (SQLException ex) {
            ex.printStackTrace();
            setMessage("SQLException on " + lsProcName + ". Please inform your System Admin.");
            p_nEditMode = EditMode.UNKNOWN;
            return false;
        }  
    }

    @Override
    public boolean UpdateTransaction() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public boolean CloseTransaction() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public boolean PostTransaction() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public boolean CancelTransaction() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
    
    @Override
    public Object getDetail(int fnRow, String fsFieldNm){
        String lsProcName = this.getClass().getSimpleName() + ".getDetail()";
        
        try {
            if (fnRow > getItemCount()){
                setMessage("Invalid row passed.");
                return null;
            }
            
            p_oDetail.absolute(fnRow);
            
            switch (fsFieldNm){
                case "sBankName":
                    return p_oDetail.getObject(18);
                default:
                    return p_oDetail.getObject(fsFieldNm);
            }
            
        } catch (SQLException ex) {
            ex.printStackTrace();
            setMessage("SQLException on " + lsProcName + ". Please inform your System Admin.");
            return null;
        }        
    }
    
    @Override
    public void setDetail(int fnRow, String fsFieldNm, Object foValue) {
        String lsProcName = this.getClass().getSimpleName() + ".setDetail()";
        
        try {
            if (p_nEditMode != EditMode.ADDNEW &&
                p_nEditMode != EditMode.UPDATE){
                System.err.println("Transaction is not on update mode.");
                setMessage("Transaction is not on update mode.");                
                return;
            }      
            
            if (fnRow > getItemCount()){
                System.err.println("Invalid row to update.");
                setMessage("Invalid row to update.");     
                return;
            }
            
            p_oDetail.absolute(fnRow);
            
            switch (fsFieldNm){
                case "sTermnlID":
                case "sCardNoxx":
                case "sApprovNo":
                case "sTermCode":
                    p_oDetail.updateObject(fsFieldNm, foValue);
                    p_oDetail.updateRow();
                    
                    p_oListener.MasterRetreive(fsFieldNm, foValue);
                    break;
                case "nAmountxx":
                    p_oDetail.updateObject(fsFieldNm, foValue);
                    p_oDetail.updateRow();
                    
                    p_oListener.MasterRetreive(fsFieldNm, foValue);
                    p_oListener.MasterRetreive("nPaymTotl", getPaymentTotal());
                    break;
                case "sBankCode":
                    getBank((String) foValue);
            }        
        } catch (SQLException e) {
            e.printStackTrace();
            setMessage("SQLException on " + lsProcName + ". Please inform your System Admin.");
        }
    }
    
    @Override
    public boolean addDetail(){
        String lsProcName = this.getClass().getSimpleName() + ".addDetail()";
        
        try {
            p_oDetail.last();
        
            if (getItemCount() > 0){
                if (String.valueOf(p_oDetail.getObject("sTermnlID")).isEmpty() ||
                    String.valueOf(p_oDetail.getObject("sBankCode")).isEmpty() ||
                    String.valueOf(p_oDetail.getObject("sCardNoxx")).isEmpty() ||
                    String.valueOf(p_oDetail.getObject("sApprovNo")).isEmpty() ||
                    ((Number) p_oDetail.getObject("nAmountxx")).doubleValue() <= 0.00){

                    setMessage("Insufficient detail on last row. Unable to add new record.");
                    return false;
                }
            }


            p_oDetail.moveToInsertRow();

            MiscUtil.initRowSet(p_oDetail);
            p_oDetail.updateObject("cTranStat", TransactionStatus.STATE_OPEN);

            p_oDetail.insertRow();
            p_oDetail.moveToCurrentRow();

            return true;
        } catch (SQLException ex) {
            ex.printStackTrace();
            setMessage("SQLException on " + lsProcName + ". Please inform your System Admin.");
        }
        
        return false;
    }
    
    @Override
    public boolean delDetail(int fnRow) {
        String lsProcName = this.getClass().getSimpleName() + ".delDetail()";
        
        try {
            if (fnRow <= 0 || fnRow > getItemCount()){
                setMessage("Invalid row item.");
                return false;
            }
            
            p_oDetail.absolute(fnRow);
            p_oDetail.deleteRow();
            
            return true;
        } catch (SQLException ex) {
            ex.printStackTrace();
            setMessage("SQLException on " + lsProcName + ". Please inform your System Admin.");
            return false;
        }
    }
    
    public JSONObject searchBanks(String fsKey, Object foValue, String fsFilter, String fsValue, boolean fbExact){
        p_oBanks.setKey(fsKey);
        p_oBanks.setValue(foValue);
        p_oBanks.setExact(fbExact);
        
        return p_oBanks.Search();
    }
    
    public ParamSearchF getSearchBanks(){
        return p_oBanks;
    }
    
    private void getBank(String foValue){
        String lsProcName = this.getClass().getSimpleName() + ".getBank()";
        
        JSONObject loJSON = searchBanks("sBankCode", foValue, "", "", true);
        if ("success".equals((String) loJSON.get("result"))){
            try {
                JSONParser loParser = new JSONParser();

                try {
                    JSONArray loArray = (JSONArray) loParser.parse((String) loJSON.get("payload"));

                    switch (loArray.size()){
                        case 0:
                            p_oDetail.updateObject("sBankCode", "");
                            p_oDetail.updateObject("sBankName", "");
                            p_oDetail.updateRow();
                            break;
                        default:
                            loJSON = (JSONObject) loArray.get(0);
                            p_oDetail.updateObject("sBankCode", (String) loJSON.get("sBankCode"));
                            p_oDetail.updateObject("sBankName", (String) loJSON.get("sBankName"));
                            p_oDetail.updateRow();
                    }
                } catch (ParseException ex) {
                    ex.printStackTrace();
                    p_oListener.MasterRetreive("sBankCode", "");
                    p_oListener.MasterRetreive("sBankName", "");
                    p_oDetail.updateRow();
                }

                p_oListener.MasterRetreive("sBankCode", (String) p_oDetail.getObject("sBankCode"));
                p_oListener.MasterRetreive("sBankName", (String) p_oDetail.getObject("sBankName"));
            } catch (SQLException ex) {
                ex.printStackTrace();
                setMessage("SQLException on " + lsProcName + ". Please inform your System Admin.");
            }
        }
    }
    
    private String getSQ_Detail(){
        return "SELECT" +
                    "  a.sTransNox" +
                    ", a.sBranchCd" +
                    ", a.sTermnlID" +
                    ", a.sBankCode" +
                    ", a.sCardNoxx" +
                    ", a.sApprovNo" +
                    ", a.nAmountxx" +
                    ", a.sTermCode" +
                    ", a.sSourceCd" +
                    ", a.sSourceNo" +
                    ", a.sCollectd" +
                    ", a.dCollectd" +
                    ", a.nBankChrg" +
                    ", a.nTaxAmtxx" +
                    ", a.nAmtPaidx" +
                    ", a.cTranStat" +
                    ", a.dModified" +
                    ", b.sBankName" +
                " FROM Credit_Card_Trans a" +
                    " LEFT JOIN Banks b ON a.sBankCode = b.sBankCode";
    }
    
    private void setMessage(String fsValue){
        p_sMessagex = fsValue;
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
}
