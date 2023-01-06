using System;
using System.Windows.Forms;
using System.Data.SqlClient;
using System.Data;
using Mantis.LVision.Win32;

namespace ANT_HAI_RPT_ShipmentList
{
    class ANT_HAI_RPT_PickingList : IReportInfo_Version_1_1
    {
        public int[] m_SelectedItemsIDs;

        public int[] SelectedItemIDs 
        {
            get
            {
                return m_SelectedItemsIDs;
            }
            set
            {
                m_SelectedItemsIDs = value;
            }
        }
        

        public object SelectReportInfo(object frm, int ReportType, object ConstructParam, DataSet dsReportSource) 
        {  
            string strReport = "";
            System.Globalization.CultureInfo InvCult;
            SqlConnection sqlQuery;
            SqlDataAdapter ad;
            string sqlText, conStr, SelectID, FirstSelectID;
            int i;
            int UserID;
            int ManagementID;
            DataSet ds1 = new DataSet();
            LVBasicForm m_Form = ((LVBasicForm)frm);
            ManagementID = m_Form.CurrentID;

            SelectID = "";
            FirstSelectID = "";
            if (m_SelectedItemsIDs != null)
            {
                for (i = 0; i <= m_SelectedItemsIDs.Length - 1; i++)
                {
                    if (i == 0)
                    {
                        SelectID = Convert.ToString(m_SelectedItemsIDs[i]);
                        FirstSelectID = Convert.ToString(m_SelectedItemsIDs[i]);
                    }
                    else
                        SelectID += "," + m_SelectedItemsIDs[i];
                }
            }
            else
            {
                string msg = "Ничего не выбрано!";
                MessageBox.Show(msg);
                return null;
            }
            
            conStr = m_Form.ConnectionString;
            InvCult = System.Globalization.CultureInfo.InstalledUICulture;
            UserID = m_Form.UserID;

            if (ConstructParam is string)
            {
                if (Convert.ToString(ConstructParam).EndsWith(".rpt"))
                    strReport = Convert.ToString(ConstructParam);
            }
            if (strReport == string.Empty)
                return null;

            if (FirstSelectID != "")
            {
                sqlText = "select r.* from LV_TaskList t with(nolock) cross apply [ANT_F_HAI_RPT_PickingList](t.tkl_ID) r where t.tkl_ID in (" + SelectID + ") order by r.ord_ID, r.tkl_ID, r.fromLocationCode";
                sqlQuery = new SqlConnection(conStr);
                ad = new SqlDataAdapter(sqlText, sqlQuery);
                ad.SelectCommand.CommandTimeout = 0;
                sqlQuery.Open();
                dsReportSource = new DataSet();
                ad.Fill(dsReportSource);

                return dsReportSource;
            }
            return null;
        }
    }
}
