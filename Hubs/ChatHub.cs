using Microsoft.AspNetCore.SignalR;
using System.Threading.Tasks;
using System;
using System.Data;
using System.Linq;
using Newtonsoft.Json.Linq;


namespace SignalR.Hubs
{
    public class ChatHub : Hub
    {
        #region "Tạo nơi lưu trữ dữ liệu"
        private static DataSet _mdtaDtbsChat;

        public ChatHub(DataSet pdtaDtbsChat)
        {
            _mdtaDtbsChat = pdtaDtbsChat;
            if (_mdtaDtbsChat.Tables.Count == 0)
            {
                crtListUser();
                crtListCntn();
                crtList_Grp();
            }
        }

        #endregion

        #region "Xử lý user connected"
        public override async Task OnConnectedAsync()
        {
            var accessToken = Context.GetHttpContext().Request.Query["access_token"];
            if (!string.IsNullOrEmpty(accessToken))
            {
                DataTable? listCntnTable = _mdtaDtbsChat.Tables["ListCntn"];
                if (listCntnTable == null)
                {
                    return;
                }

                DataRow lrowUserItem = listCntnTable.NewRow();
                lrowUserItem["UserCode"] = accessToken;
                lrowUserItem["CntnCode"] = Context.ConnectionId;
                listCntnTable.Rows.Add(lrowUserItem);

                var onlineUsers = listCntnTable?.Select()
                                                  .Select(row => new
                                                  {
                                                      CntnCode = row["CntnCode"],
                                                      UserCode = row["UserCode"]
                                                  })
                                                  .ToArray();

                await Clients.All.SendAsync("UpdateOnlineUsers", onlineUsers);
            }
            await base.OnConnectedAsync();
        }
        #endregion

        #region "Xử lý user diconnected"
        public override async Task OnDisconnectedAsync(Exception? exception)
        {
            string lstrCntnCode = Context.ConnectionId;
            DataTable? listCntnTable = _mdtaDtbsChat.Tables["ListCntn"];
            if (listCntnTable == null)
            {
                await base.OnDisconnectedAsync(exception);
                return;
            }

            DataRow? lrowCntnItem = listCntnTable.Rows.Find(lstrCntnCode);

            if (lrowCntnItem != null)
            {
                string? userCode = lrowCntnItem["UserCode"]?.ToString();
                listCntnTable.Rows.Remove(lrowCntnItem);
            }

            var onlineUsers = listCntnTable?.Select()
                                                 .Select(row => new
                                                 {
                                                     CntnCode = row["CntnCode"],
                                                     UserCode = row["UserCode"]
                                                 })
                                                 .ToArray();

            await Clients.All.SendAsync("UpdateOnlineUsers", onlineUsers);

            await base.OnDisconnectedAsync(exception);
        }
        #endregion

        public async Task SendMessageAsync(string user, string message)
        {
            await Clients.All.SendAsync("ReceiveMessage", user, message);
        }

        public async Task JoinGroup(string userId, string groupId, JArray members)
        {
            DataTable listJoinTable = new DataTable();
            listJoinTable.Columns.Add("UserCode", typeof(string));
            listJoinTable.Columns.Add("UserCntn", typeof(string));

            DataRow newRow = listJoinTable.NewRow();
            newRow["UserCode"] = userId;
            newRow["UserCntn"] = Context.ConnectionId;
            listJoinTable.Rows.Add(newRow);


            DataTable? list_GrpTable = _mdtaDtbsChat.Tables["List_Grp"];
            if (list_GrpTable == null)
            {
                return;
            }
            DataRow lrowUserItem = list_GrpTable.NewRow();
            lrowUserItem["Grp_Code"] = groupId;
            lrowUserItem["ListJoin"] = listJoinTable;
            lrowUserItem["ListMebr"] = string.Join(",", members);
            list_GrpTable.Rows.Add(lrowUserItem);



            var UpdateGroup = list_GrpTable.Select().Select(row => new { Grp_Code = row["Grp_Code"], ListJoin = row["ListJoin"], ListMebr = row["ListMebr"] }).ToArray();
            await Clients.All.SendAsync("UpdateGroup", UpdateGroup);

        }

        #region "Private methods"
        private void crtListUser()
        {
            DataColumn lobjUserCode;
            DataTable ltblListUser = new DataTable("ListUser");
            lobjUserCode = ltblListUser.Columns.Add("UserCode", typeof(string));
            //ltblUserCntn.Columns.Add("CntnIdtf", typeof(string));
            ltblListUser.PrimaryKey = new DataColumn[] { lobjUserCode };
            _mdtaDtbsChat.Tables.Add(ltblListUser);
        }
        private void crtListCntn()
        {
            DataColumn lobjCntnCode;
            DataTable ltblListCntn = new DataTable("ListCntn");
            lobjCntnCode = ltblListCntn.Columns.Add("CntnCode", typeof(string));
            ltblListCntn.Columns.Add("UserCode", typeof(string));
            ltblListCntn.PrimaryKey = new DataColumn[] { lobjCntnCode };
            _mdtaDtbsChat.Tables.Add(ltblListCntn);
        }
        private void crtList_Grp()
        {
            DataColumn lobjGrp_Code;
            DataTable ltblList_Grp = new DataTable("List_Grp");

            lobjGrp_Code = ltblList_Grp.Columns.Add("Grp_Code", typeof(string));

            // ListJoin column
            DataColumn listJoinColumn = ltblList_Grp.Columns.Add("ListJoin", typeof(DataTable));
            listJoinColumn.DataType = typeof(DataTable);

            // ListMebr column
            DataColumn listMebrColumn = ltblList_Grp.Columns.Add("ListMebr", typeof(string));
            listMebrColumn.DataType = typeof(string);

            ltblList_Grp.PrimaryKey = new DataColumn[] { lobjGrp_Code };
            _mdtaDtbsChat.Tables.Add(ltblList_Grp);
        }
        #endregion

    }
}
