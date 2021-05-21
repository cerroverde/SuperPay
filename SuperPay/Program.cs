using Oracle.ManagedDataAccess.Client;
using System;
using System.Data;
using VistaDB.Provider;


namespace SuperPay
{
    class Program
    {
        static void Main(string[] args)
        {
            Program objProgram = new Program();
            DataTable DTemployees = objProgram.Employee();
            DataTable DTnivalues = objProgram.NiValues();
            DataTable DTpayitems = objProgram.PayItems();
            DataTable DTpensionvalues = objProgram.PensionValues();
            DataTable DTsmpvalues = objProgram.SmpValues();

            objProgram.ParseVistaDB("Employees", "TMP_GB_SPP_EMPLOYEE", DTemployees);
            objProgram.ParseVistaDB("NIValues", "TMP_GB_SPP_NIVALUES", DTnivalues);
            objProgram.ParseVistaDB("PayItems", "TMP_GB_SPP_PAYITEMS", DTpayitems);
            objProgram.ParseVistaDB("PensionValues", "TMP_GB_SPP_PENSIONVALUES", DTpensionvalues);
            objProgram.ParseVistaDB("SMPValues", "TMP_GB_SPP_SMPVALUES", DTsmpvalues);


        }

        public void ParseVistaDB(string vistaTableName, string qualifiedTableName, DataTable dataTable)
        {
            Program objProgram = new Program();
            //Create a connection to VistaDB
            string connectionString = @"Data Source=C:\superpay\Superpay4.vdb6;Open Mode=SingleProcessReadWrite";

            //Create string connection to Oracle
            string conString = "User Id=dwrrhh; password=milenaok;" +

            //How to connect to an Oracle DB without SQL*Net configuration file
            //also known as tnsnames.ora.
            "Data Source=cocgsa016.cupagroup.com:1521/CUPIRE; Pooling=false;";

            try
            {
                //Read data from VistaDb and create variable with Values 
                using (VistaDBConnection dbConn = new VistaDBConnection(connectionString))
                {

                    dbConn.Open();
                    VistaDBCommand command = dbConn.CreateCommand();
                    command.CommandText = "SELECT * FROM " + vistaTableName;

                    using (VistaDBDataReader dr = command.ExecuteReader())
                    {
                        int count = 0;
                        while (dr.Read())
                        {
                            count++;
                            DataRow dataRow = dataTable.NewRow();
                            for (int i = 0; dataTable.Columns.Count > i; i++)
                            {
                                string colName = dataTable.Columns[i].ColumnName.ToString();
                                int colLength = dr[colName].ToString().Length;

                                if (colLength == 0)
                                {
                                    dataRow[colName] = 0.0;
                                }
                                else
                                {
                                    string s1 = dr[colName].ToString();
                                    string s2 = s1.Replace(",", ".");

                                    dataRow[colName] = s2;
                                }

                                //Console.WriteLine("Columna Nº: " + count + "-- Nombre COL: " + colName + " -- Valor: " + dataRow[colName]);
                            }
                            dataTable.Rows.Add(dataRow);
                        }

                        // Send parse data to Oracle
                        objProgram.WriteToOracle(qualifiedTableName, conString, dataTable);
                    }

                    // Just a little pause for viewing 
                    //Console.ReadLine();
                }

            }
            catch (Exception e)
            {
                Console.WriteLine("Error encountered during parsed data from VistaDB table name "
                    + vistaTableName + " con error " + e.Message);
                throw;
            }
            finally
            {
                Console.WriteLine("Parse data from Table " + vistaTableName + " of VistaDb was successfully");
            }
        }

        public void WriteToOracle(string qualifiedTableName, string conString, DataTable dataTable)
        {
            try
            {
                //Oracle connection
                OracleConnection con = new OracleConnection();
                con.ConnectionString = conString;
                con.Open();

                // First truncate table
                OracleCommand truncateTbl = con.CreateCommand();
                truncateTbl.CommandText = "TRUNCATE table " + qualifiedTableName;
                OracleDataReader dataReader = truncateTbl.ExecuteReader();

                // them bulk data
                using (OracleBulkCopy bulkCopy = new OracleBulkCopy(con))
                {
                    bulkCopy.DestinationTableName = qualifiedTableName;
                    bulkCopy.BulkCopyTimeout = 600;
                    bulkCopy.WriteToServer(dataTable);
                }
                con.Close();
                con.Dispose();
            }
            catch (Exception e)
            {
                Console.WriteLine("Error encountered during Bulk INSERT operation " + e.Message);
                throw;
            }
            finally
            {
                Console.WriteLine("Volcado de datos en Oracle table " + qualifiedTableName + " satisfactorio");
            }
        }

        public DataTable SmpValues()
        {
            DataTable dataTable = new DataTable();
            dataTable.Columns.Add("SMPVID");
            dataTable.Columns.Add("EEDATAID");
            dataTable.Columns.Add("PAYTYPE");
            dataTable.Columns.Add("BABYDUEDATE");
            dataTable.Columns.Add("MPPSTARTDATE");
            dataTable.Columns.Add("ASPPENDDATE");
            dataTable.Columns.Add("SMPSTOPDATE");
            dataTable.Columns.Add("MAPAID");
            dataTable.Columns.Add("CANPAYPARTWEEKS");
            dataTable.Columns.Add("BABYBORNDATE");
            dataTable.Columns.Add("ISSTILLBORN");
            dataTable.Columns.Add("PRISTARTDATE");
            dataTable.Columns.Add("NOTIFIEDMPPSTARTDATE");
            dataTable.Columns.Add("WEEKSWORKED");
            dataTable.Columns.Add("ISASPPINVALID");
            dataTable.Columns.Add("ISEESTARTTOOLATE");
            dataTable.Columns.Add("ISEELEAVETOOEARLY");
            dataTable.Columns.Add("ISPAYBELOWLEL");
            dataTable.Columns.Add("ISMPPSTARTED");
            dataTable.Columns.Add("AVERAGEPAY");
            dataTable.Columns.Add("TOTALWEEKS");
            dataTable.Columns.Add("TOTALDAYS");
            dataTable.Columns.Add("TOTALSMP");
            dataTable.Columns.Add("TOTALSMPTHISYEAR");
            dataTable.Columns.Add("LASTWEEKS");
            dataTable.Columns.Add("LASTDAYS");
            dataTable.Columns.Add("LASTSMP");
            dataTable.Columns.Add("CALCDATE");

            return dataTable;
        }

        public DataTable PensionValues()
        {
            DataTable dataTable = new DataTable();
            dataTable.Columns.Add("PVID");
            dataTable.Columns.Add("EEDATAID");
            dataTable.Columns.Add("SEQ");
            dataTable.Columns.Add("PTYPE");
            dataTable.Columns.Add("EEDEDNIND");
            dataTable.Columns.Add("EEDEDNVAL");
            dataTable.Columns.Add("ERDEDNIND");
            dataTable.Columns.Add("ERDEDNVAL");
            dataTable.Columns.Add("LOWERLIMIT");
            dataTable.Columns.Add("UPPERLIMIT");
            dataTable.Columns.Add("PENCPYID");
            dataTable.Columns.Add("PENCPYREF");
            dataTable.Columns.Add("LASTEE");
            dataTable.Columns.Add("LASTER");
            dataTable.Columns.Add("TOTALEE");
            dataTable.Columns.Add("TOTALER");
            dataTable.Columns.Add("EEDEDNVALA");
            dataTable.Columns.Add("ERDEDNVALA");
            dataTable.Columns.Add("YTDEE");
            dataTable.Columns.Add("YTDER");
            dataTable.Columns.Add("LASTCALCGRS");
            dataTable.Columns.Add("TOTALSAC");
            dataTable.Columns.Add("EENOTDEDUCTED");
            dataTable.Columns.Add("ERNOTDEDUCTED");

            return dataTable;
        }

        public DataTable PayItems()
        {
            DataTable dataTable = new DataTable();
            dataTable.Columns.Add("PIID");
            dataTable.Columns.Add("EEDATAID");
            dataTable.Columns.Add("POSN");
            dataTable.Columns.Add("PSOFS");
            dataTable.Columns.Add("PIT");
            dataTable.Columns.Add("SUBTO");
            dataTable.Columns.Add("DESCRIPTION");
            dataTable.Columns.Add("PHNUM");
            dataTable.Columns.Add("RATE");
            dataTable.Columns.Add("HASNUMUNITS");
            dataTable.Columns.Add("NUMUNITS");
            dataTable.Columns.Add("HASAMNT");
            dataTable.Columns.Add("AMNT");
            dataTable.Columns.Add("ISMW");
            dataTable.Columns.Add("WASMW");

            return dataTable;
        }

        public DataTable NiValues()
        {
            DataTable dataTable = new DataTable();
            dataTable.Columns.Add("NIVID");
            dataTable.Columns.Add("EEDATAID");
            dataTable.Columns.Add("POSN");
            dataTable.Columns.Add("SEQ");
            dataTable.Columns.Add("CODE");
            dataTable.Columns.Add("ISDIRECTOR");
            dataTable.Columns.Add("EENI");
            dataTable.Columns.Add("ERNI");
            dataTable.Columns.Add("PBTOLEL");
            dataTable.Columns.Add("PBTOPT");
            dataTable.Columns.Add("PBTOUAP");
            dataTable.Columns.Add("PBTOUEL");
            dataTable.Columns.Add("PBOVERUEL");
            dataTable.Columns.Add("EEREBATE");
            dataTable.Columns.Add("ERREBATE");
            dataTable.Columns.Add("SPARE1");
            dataTable.Columns.Add("SPARE2");
            dataTable.Columns.Add("PBTOAUST");

            return dataTable;
        }

        public DataTable Employee()
        {
            DataTable dataTable = new DataTable();
            dataTable.Columns.Add("EEDATAID");
            dataTable.Columns.Add("EEID");
            dataTable.Columns.Add("TAXYEAR");
            dataTable.Columns.Add("LRPYEAR");
            dataTable.Columns.Add("LRPPAYINT");
            dataTable.Columns.Add("LRPPERIOD");
            dataTable.Columns.Add("LRTAXPERIOD");
            dataTable.Columns.Add("NUMPER");
            dataTable.Columns.Add("RECTYPE");
            dataTable.Columns.Add("ARCSEQ");
            dataTable.Columns.Add("CANCELSTATUS");
            dataTable.Columns.Add("CPYID");
            dataTable.Columns.Add("EENUM");
            dataTable.Columns.Add("DEPT");
            dataTable.Columns.Add("ISDELETED");
            dataTable.Columns.Add("FILEP45ONLINE");
            dataTable.Columns.Add("ISP45FILED");
            dataTable.Columns.Add("ISHELD");
            dataTable.Columns.Add("ISCASUAL");
            dataTable.Columns.Add("ISPAYE");
            dataTable.Columns.Add("TITLE");
            dataTable.Columns.Add("FNAME1");
            dataTable.Columns.Add("FNAME2");
            dataTable.Columns.Add("SNAME");
            dataTable.Columns.Add("PAYINT");
            dataTable.Columns.Add("LASTRUNDATE");
            dataTable.Columns.Add("BIRTHDATE");
            dataTable.Columns.Add("STARTDATE");
            dataTable.Columns.Add("LEAVEDATE");
            dataTable.Columns.Add("DEATHDATE");
            dataTable.Columns.Add("WORKSNUM");
            dataTable.Columns.Add("JOBTITLE");
            dataTable.Columns.Add("NINO");
            dataTable.Columns.Add("GENDER");
            dataTable.Columns.Add("SCON");
            dataTable.Columns.Add("NOTES");
            dataTable.Columns.Add("COMMENT");
            dataTable.Columns.Add("DOCPW");
            dataTable.Columns.Add("ACCRUALIND");
            dataTable.Columns.Add("ACCRUALVAL");
            dataTable.Columns.Add("LOANDUE");
            dataTable.Columns.Add("LOANREPAY");
            dataTable.Columns.Add("NPROUNDTO");
            dataTable.Columns.Add("PAYMETH1");
            dataTable.Columns.Add("PAYMETH2");
            dataTable.Columns.Add("SPLITPAYIND");
            dataTable.Columns.Add("SPLITPAYVAL");
            dataTable.Columns.Add("TCTYPE");
            dataTable.Columns.Add("TCPREFIX");
            dataTable.Columns.Add("TCCODE");
            dataTable.Columns.Add("TCSUFFIX");
            dataTable.Columns.Add("TCISWEEK1");
            dataTable.Columns.Add("TCISSCOTTISH");
            dataTable.Columns.Add("TCLCSRC");
            dataTable.Columns.Add("TCLCDATE");
            dataTable.Columns.Add("HASSTUDENTLOAN");
            dataTable.Columns.Add("NCODE");
            dataTable.Columns.Add("ISDIRECTOR");
            dataTable.Columns.Add("DIRSTARTDATE");
            dataTable.Columns.Add("HASAPP");
            dataTable.Columns.Add("SSPWORKPATTERN");
            dataTable.Columns.Add("B4DPIWSTART");
            dataTable.Columns.Add("B4DLASTSICK");
            dataTable.Columns.Add("B4DNUMWEEKS");
            dataTable.Columns.Add("B4DWAITDAYSUSED");
            dataTable.Columns.Add("B4DPAID");
            dataTable.Columns.Add("B4DPAIDTY");
            dataTable.Columns.Add("PREVEMPPAY");
            dataTable.Columns.Add("PREVEMPTAX");
            dataTable.Columns.Add("TOTALGROSS");
            dataTable.Columns.Add("TOTALGROSSFORTAX");
            dataTable.Columns.Add("TOTALGROSSFORNI");
            dataTable.Columns.Add("TOTALGROSSFORPEN");
            dataTable.Columns.Add("TOTALSSPDAYS");
            dataTable.Columns.Add("TOTALSSP");
            dataTable.Columns.Add("TOTALSSPTHISYEAR");
            dataTable.Columns.Add("TOTALTAX");
            dataTable.Columns.Add("TOTALACCRUAL");
            dataTable.Columns.Add("TOTALSTUDENTLOAN");
            dataTable.Columns.Add("TOTALNET");
            dataTable.Columns.Add("LASTGROSS");
            dataTable.Columns.Add("LASTGROSSFORTAX");
            dataTable.Columns.Add("LASTGROSSFORNI");
            dataTable.Columns.Add("LASTGROSSFORPEN");
            dataTable.Columns.Add("LASTSSPDAYS");
            dataTable.Columns.Add("LASTSSP");
            dataTable.Columns.Add("LASTTAX");
            dataTable.Columns.Add("LASTACCRUAL");
            dataTable.Columns.Add("LASTACCRUALPAID");
            dataTable.Columns.Add("LASTSTUDENTLOAN");
            dataTable.Columns.Add("LASTLOANPAID");
            dataTable.Columns.Add("LASTNPROUNDTO");
            dataTable.Columns.Add("LASTNPROUNDBF");
            dataTable.Columns.Add("LASTNPROUNDCF");
            dataTable.Columns.Add("LASTNET");
            dataTable.Columns.Add("EASSTATUS");
            dataTable.Columns.Add("FPS1STATUS");
            dataTable.Columns.Add("ISPAYAFTERLEAVE");
            dataTable.Columns.Add("ISEXPAT");
            dataTable.Columns.Add("ISONSTRIKE");
            dataTable.Columns.Add("HASUNPAIDABSENCE");
            dataTable.Columns.Add("HOURS");
            dataTable.Columns.Add("OLDPAYROLLID");
            dataTable.Columns.Add("EENAMEFORMAT");
            dataTable.Columns.Add("PASSPORTNUMBER");
            dataTable.Columns.Add("PARTNERNINO");
            dataTable.Columns.Add("PNTITLE");
            dataTable.Columns.Add("PNFNAME1");
            dataTable.Columns.Add("PNFNAME2");
            dataTable.Columns.Add("PNSNAME");
            dataTable.Columns.Add("BACSHASHSRC");
            dataTable.Columns.Add("DIRCALC");
            dataTable.Columns.Add("HASOCCPEN");
            dataTable.Columns.Add("ISOCCPBEREAVED");
            dataTable.Columns.Add("OCCPENAMOUNT");
            dataTable.Columns.Add("HASSTATEPEN");
            dataTable.Columns.Add("ISSTATEPBEREAVED");
            dataTable.Columns.Add("STATEPENAMOUNT");
            dataTable.Columns.Add("ISSECONDED");
            dataTable.Columns.Add("SECTYPE");
            dataTable.Columns.Add("ISEEA");
            dataTable.Columns.Add("ISEPM6");
            dataTable.Columns.Add("ISFPSFILED");
            dataTable.Columns.Add("P46STATE");
            dataTable.Columns.Add("STLEAVEDATE");
            dataTable.Columns.Add("STTCTYPE");
            dataTable.Columns.Add("STTCPREFIX");
            dataTable.Columns.Add("STTCCODE");
            dataTable.Columns.Add("STTCSUFFIX");
            dataTable.Columns.Add("STTCISWEEK1");
            dataTable.Columns.Add("STTCISSCOTTISH");
            dataTable.Columns.Add("STPAY");
            dataTable.Columns.Add("STTAX");
            dataTable.Columns.Add("STLRYEAR");
            dataTable.Columns.Add("STLRPAYINT");
            dataTable.Columns.Add("STLRPERIOD");
            dataTable.Columns.Add("TOTALGROSSFORBEN");
            dataTable.Columns.Add("LASTGROSSFORBEN");
            dataTable.Columns.Add("PWTYPE");
            dataTable.Columns.Add("PEDEFERTO");
            dataTable.Columns.Add("TOTALFOREIGNTAX");
            dataTable.Columns.Add("TUPEDATE");
            dataTable.Columns.Add("PAYREFPERSTART");
            dataTable.Columns.Add("PAYREFPEREND");
            dataTable.Columns.Add("PENREF1");
            dataTable.Columns.Add("PENREF2");
            dataTable.Columns.Add("PENSCHEMECHOICE");
            dataTable.Columns.Add("PENSACRIFICE1");
            dataTable.Columns.Add("PENSACRIFICE2");
            dataTable.Columns.Add("PENREF3");
            dataTable.Columns.Add("PENREF4");
            dataTable.Columns.Add("PENREF5");
            dataTable.Columns.Add("PENREF6");
            dataTable.Columns.Add("LASTGROSSFORASSESS");
            dataTable.Columns.Add("ISAEWORKER");
            dataTable.Columns.Add("SENDEMAIL");
            dataTable.Columns.Add("PRPOPTION");
            dataTable.Columns.Add("PSTYPES");
            dataTable.Columns.Add("ISINTERMEDIARY");
            dataTable.Columns.Add("INTWENG");
            dataTable.Columns.Add("INTWUTR");
            dataTable.Columns.Add("INTINCLVAT");
            dataTable.Columns.Add("INTPAIDNAME");
            dataTable.Columns.Add("INTCHREG");
            dataTable.Columns.Add("PENFLEXDD");
            dataTable.Columns.Add("TEACHMEMREF");
            dataTable.Columns.Add("TEACHLANUM");
            dataTable.Columns.Add("TEACHSCHOOLNUM");
            dataTable.Columns.Add("TEACHDAYSEXC");
            dataTable.Columns.Add("TEACHREGULAR");
            dataTable.Columns.Add("TEACHFTSAL");
            dataTable.Columns.Add("TEACHPTSAL");
            dataTable.Columns.Add("TEACHADDCONTRIB");
            dataTable.Columns.Add("TEACHOTIME");
            dataTable.Columns.Add("HASPOSTGRADLOAN");
            dataTable.Columns.Add("TAXCODE");
            dataTable.Columns.Add("STTAXCODE");
            dataTable.Columns.Add("TOTALPGLOAN");
            dataTable.Columns.Add("TOTALDISGREM");
            dataTable.Columns.Add("LASTPGLOAN");
            dataTable.Columns.Add("LASTDISGREM");
            dataTable.Columns.Add("FLEXPENRIGHTS");
            dataTable.Columns.Add("FLEXDEATHBEN");
            dataTable.Columns.Add("FLEXILLHEALTH");
            dataTable.Columns.Add("FLEXTAXABLE");
            dataTable.Columns.Add("FLEXNONTAXABLE");
            dataTable.Columns.Add("APPRSTARTDATE");
            dataTable.Columns.Add("EARNPERENDDATE");
            dataTable.Columns.Add("HPAVAILABLEDAYS");
            dataTable.Columns.Add("HPCFDAYS");
            dataTable.Columns.Add("TOTALHPDAYSTAKEN");
            dataTable.Columns.Add("LASTHPDAYSTAKEN");
            dataTable.Columns.Add("PSPROGVER");

            return dataTable;
        }
    }
}
