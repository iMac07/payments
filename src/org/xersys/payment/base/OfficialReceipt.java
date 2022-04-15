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
import org.xersys.clients.search.ClientSearch;
import org.xersys.commander.contants.EditMode;
import org.xersys.commander.contants.TransactionStatus;
import org.xersys.commander.iface.LRecordMas;
import org.xersys.commander.iface.XNautilus;
import org.xersys.commander.iface.XPaymentInfo;
import org.xersys.commander.iface.XPayments;
import org.xersys.commander.util.MiscUtil;
import org.xersys.commander.util.SQLUtil;
import org.xersys.commander.util.StringUtil;

public class OfficialReceipt implements XPayments{
    private final String MASTER_TABLE = "Receipt_Master";
    private final double VAT_RATE = 0.12;
    
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
    private XPaymentInfo p_oCard;
    private XPaymentInfo p_oCheque;
    private XPaymentInfo p_oGC;
    
    private final ClientSearch p_oSearchClient;
    
    public OfficialReceipt(XNautilus foNautilus, String fsBranchCd, boolean fbWithParent){
        p_oNautilus = foNautilus;
        p_sBranchCd = fsBranchCd;
        p_bWithParent = fbWithParent;
        
        p_oSearchClient = new ClientSearch(p_oNautilus, ClientSearch.SearchType.searchClient);
        
        p_sSourceCd = "";
        p_sSourceNo = "";
        p_sClientID = "";
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
                p_oMaster.updateObject("sClientID", loRS1.getString("sClientID"));
                p_oMaster.updateObject("sClientNm", loRS1.getString("sClientNm"));                
                p_oMaster.updateObject("nTranTotl", loRS1.getDouble("nTranTotl"));
                p_oMaster.updateObject("nDiscount", loRS1.getDouble("nDiscount"));
                p_oMaster.updateObject("nAddDiscx", loRS1.getDouble("nAddDiscx"));
                p_oMaster.updateObject("nFreightx", loRS1.getDouble("nFreightx"));
                p_oMaster.updateObject("nAmtPaidx", loRS1.getDouble("nAmtPaidx"));                
                p_oMaster.updateObject("cTranStat", TransactionStatus.STATE_OPEN);    
                p_oMaster.updateRow();
                
                if (!p_sClientID.isEmpty()) 
                    setMaster("sClientID", p_sClientID);
                else if (loRS1.getString("sClientID").equals(""))
                    setMaster("sClientID", loRS1.getString("sClientID"));
                
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
    public boolean SaveTransaction() {
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
            
            p_oMaster.first();
            
            if (p_oMaster.getString("sClientID").isEmpty()){
                setMessage("Client must not be empty.");
                return false;
            }
            
            if (p_oMaster.getString("sInvNumbr").isEmpty()){
                setMessage("OR number is not set..");
                return false;
            }
            
            if (!p_bWithParent) p_oNautilus.beginTrans();
        
            if (p_nEditMode == EditMode.ADDNEW){
                Connection loConn = getConnection();

                p_oMaster.updateObject("sTransNox", MiscUtil.getNextCode(MASTER_TABLE, "sTransNox", true, loConn, p_sBranchCd));
                p_oMaster.updateObject("sBranchCd", (String) p_oNautilus.getSysConfig("sBranchCd"));
                p_oMaster.updateObject("dTransact", p_oNautilus.getServerDate());
                p_oMaster.updateObject("sSourceCd", p_sSourceCd);
                p_oMaster.updateObject("sSourceNo", p_sSourceNo);
                p_oMaster.updateObject("dModified", p_oNautilus.getServerDate());
                p_oMaster.updateRow();
                
                if (!p_bWithParent) MiscUtil.close(loConn);          
                
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
                
                lsSQL = MiscUtil.rowset2SQL(p_oMaster, MASTER_TABLE, "sClientNm;nTranTotl;nDiscount;nAddDiscx;nFreightx;nAmtPaidx");
            } else {
                
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
            String lsSQL = "UPDATE " + MASTER_TABLE + " SET" +
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
    public boolean PrintTransaction() {
        try {
            //print invoice here
            if ("0".equals((String) p_oMaster.getObject("cTranStat"))){
                if (!CloseTransaction()) return false;
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
                    getClient("a.sClientID", foValue);
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
    public JSONObject searchClient(String fsKey, Object foValue, boolean fbExact){
        p_oSearchClient.setKey(fsKey);
        p_oSearchClient.setValue(foValue);
        p_oSearchClient.setExact(fbExact);
        
        return p_oSearchClient.Search();
    }
    
    @Override
    public ClientSearch getSearchClient(){
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
    
    private boolean updateSource() throws SQLException{
        String lsSQL = "";
        ResultSet loRS;
        
        double lnPaymTotl = computePaymentTotal();
        
        switch(p_sSourceCd){
            case "CO":
                lsSQL = "SELECT" +
                            " (nTranTotl - ((nTranTotl * nDiscount / 100) + nAddDiscx) + nFreightx - nAmtPaidx) xPayablex" +
                            ", sSourceCd" +
                            ", sSourceNo" +
                        " FROM SP_Sales_Order_Master" +
                        " WHERE sTransNox = " + SQLUtil.toSQL(p_sSourceNo);
                
                loRS = p_oNautilus.executeQuery(lsSQL);
                if (loRS.next()){                    
                    lsSQL = "UPDATE SP_Sales_Order_Master SET" +
                            "  nAmtPaidx = nAmtPaidx + " + lnPaymTotl;
                    
                    if ((double) getMaster("nAdvPaymx") > 0.00){
                        lsSQL += ", nForCredt = nForCredt + " + lnPaymTotl +
                                    ", nAvailBal = nAvailBal + " + lnPaymTotl;
                    }
                    
                    if (loRS.getDouble("xPayablex") <= lnPaymTotl)
                        lsSQL += ", cTranStat = '2'";
                    else 
                        lsSQL += ", cTranStat = '1'";
                    
                    MiscUtil.close(loRS);
                    lsSQL = MiscUtil.addCondition(lsSQL, "sTransNox = " + SQLUtil.toSQL(p_sSourceNo));
                    
                    if (!lsSQL.isEmpty()){
                        if(p_oNautilus.executeUpdate(lsSQL, "SP_Sales_Order_Master", p_sBranchCd, "") <= 0){
                            if(!p_oNautilus.getMessage().isEmpty())
                                setMessage(p_oNautilus.getMessage());
                            else
                                setMessage("Unable to update source transaction.");

                            return false;
                        }
                    }
                    
                    
                    return true;
                }
                break;
            case "JO":
                lsSQL = "SELECT" +
                            " (nTranTotl - ((nTranTotl * nDiscount / 100) + nAddDiscx) + nFreightx - nAmtPaidx) xPayablex" +
                            ", sSourceCd" +
                            ", sSourceNo" +
                        " FROM Job_Order_Master" +
                        " WHERE sTransNox = " + SQLUtil.toSQL(p_sSourceNo);
                
                loRS = p_oNautilus.executeQuery(lsSQL);
                if (loRS.next()){                    
                    lsSQL = "UPDATE Job_Order_Master SET" +
                            "  nAmtPaidx = nAmtPaidx + " + lnPaymTotl;
                    
                    if (loRS.getDouble("xPayablex") <= lnPaymTotl)
                        lsSQL += ", cTranStat = '2'";
                    else 
                        lsSQL += ", cTranStat = '1'";
                    
                    lsSQL = MiscUtil.addCondition(lsSQL, "sTransNox = " + SQLUtil.toSQL(p_sSourceNo));
                    
                    if (!lsSQL.isEmpty()){
                        if(p_oNautilus.executeUpdate(lsSQL, "Job_Order_Master", p_sBranchCd, "") <= 0){
                            if(!p_oNautilus.getMessage().isEmpty())
                                setMessage(p_oNautilus.getMessage());
                            else
                                setMessage("Unable to update source transaction.");

                            return false;
                        }
                    }
                    
                    //update source of the source
                    if (!loRS.getString("sSourceCd").isEmpty() && !loRS.getString("sSourceNo").isEmpty()){
                        if (loRS.getString("sSourceCd").equals("JEst")){
                            lsSQL = "UPDATE Job_Estimate_Master SET" +
                                        " cTranStat = '4'" + 
                                    " WHERE sTransNox = " + SQLUtil.toSQL(loRS.getString("sSourceNo"));

                            if (!lsSQL.isEmpty()){
                                if(p_oNautilus.executeUpdate(lsSQL, "Job_Estimate_Master", p_sBranchCd, "") <= 0){
                                    if(!p_oNautilus.getMessage().isEmpty())
                                        setMessage(p_oNautilus.getMessage());
                                    else
                                        setMessage("Unable to update source transaction.");

                                    return false;
                                }
                            }
                        }
                    }
                    
                    MiscUtil.close(loRS);
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
                (double) p_oMaster.getObject("nAdvPaymx") +
                            p_oCard.getPaymentTotal();
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
                " FROM Receipt_Master a" +
                    " LEFT JOIN Client_Master b ON a.sClientID = b.sClientID" +
                    " LEFT JOIN Job_Order_Master c ON a.sSourceNo = c.sSourceNo";
    }
    
    private String getSQ_Source(){
        String lsSQL = "";
        
        switch (p_sSourceCd){
            case "JO": //job order
                lsSQL = "SELECT" +
                        "  IFNULL(c.sClientNm, '') sClientNm" +
                        ", a.nLabrTotl nTranTotl" +
                        ", a.nDiscount" +
                        ", a.nAddDiscx" +
                        ", a.nFreightx" +
                        ", a.nLabrPaid" +
                        ", b.sSourceCd" +
                        ", a.sTransNox" +
                        ", a.sClientID" +
                    " FROM Job_Order_Master a" +
                        " LEFT JOIN xxxTempTransactions b" +
                            " ON b.sSourceCd = 'JO'" +
                                " AND a.sTransNox = b.sTransNox" + 
                        " LEFT JOIN Client_Master c" + 
                            " ON a.sMechanic = c.sClientID" +
                    " WHERE a.sTransNox = " + SQLUtil.toSQL(p_sSourceNo);
            case "CO": //customer order
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
                    " FROM SP_Sales_Order_Master a" +
                        " LEFT JOIN xxxTempTransactions b" +
                            " ON b.sSourceCd = 'CO'" +
                                " AND a.sTransNox = b.sTransNox" + 
                        " LEFT JOIN Client_Master c" + 
                            " ON a.sClientID = c.sClientID" +
                    " WHERE a.sTransNox = " + SQLUtil.toSQL(p_sSourceNo);
        }
        
        return lsSQL;
    }
}
