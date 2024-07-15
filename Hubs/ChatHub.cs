using Microsoft.AspNetCore.SignalR;
using System.Threading.Tasks;
using System;
using System.Data;
using System.Linq;
using Newtonsoft.Json.Linq;
using System.Collections.Generic;


namespace SignalR.Hubs
{
    public class ChatHub : Hub
    {
        #region "Tạo nơi lưu trữ dữ liệu"
        private static DataSet _mdtaDtbsChat;

        public ChatHub(DataSet pdtaDtbsChat)
        {
            _mdtaDtbsChat = pdtaDtbsChat ?? new DataSet();
            if (_mdtaDtbsChat.Tables.Count == 0)
            {
                crtListUser();
                crtListCntn();
                crtList_Grp();
            }
        }

        #endregion

        #region "Xử lý sự kiện user connected"
        public override async Task OnConnectedAsync()
        {
            var accessToken = Context.GetHttpContext().Request.Query["access_token"];
            if (!string.IsNullOrEmpty(accessToken))
            {
                var listCntnTable = _mdtaDtbsChat.Tables["ListCntn"];
                if (listCntnTable != null)
                {
                    var lrowUserItem = listCntnTable.NewRow();
                    lrowUserItem["UserCode"] = accessToken;
                    lrowUserItem["CntnCode"] = Context.ConnectionId;
                    listCntnTable.Rows.Add(lrowUserItem);
                    await UpdateOnlineUsers(listCntnTable);
                }
            }
            await base.OnConnectedAsync();
        }
        #endregion

        #region "Xử lý sự kiện user diconnected"
        public override async Task OnDisconnectedAsync(Exception exception)
        {
            var listCntnTable = _mdtaDtbsChat.Tables["ListCntn"];
            var list_GrpTable = _mdtaDtbsChat.Tables["List_Grp"];
            if (listCntnTable != null)
            {
                var lrowCntnItem = listCntnTable.Rows.Find(Context.ConnectionId);
                if (lrowCntnItem != null)
                {
                    var existingGrp_Code = lrowCntnItem["Grp_Code"].ToString();
                    var existingUserCode = lrowCntnItem["UserCode"].ToString();
                    if (!string.IsNullOrEmpty(existingGrp_Code) && !string.IsNullOrEmpty(existingUserCode) && list_GrpTable != null)
                    {
                        await UpdateGroupOnLeave(list_GrpTable, listCntnTable, existingGrp_Code, existingUserCode);
                    }
                    listCntnTable.Rows.Remove(lrowCntnItem);
                    await UpdateOnlineUsers(listCntnTable);
                }
            }
            await base.OnDisconnectedAsync(exception);
        }
        #endregion

        #region "Xử lý sự kiện vào group"
        public async Task JoinGroup(string userId, string groupId, string members)
        {
            var listCntnTable = _mdtaDtbsChat.Tables["ListCntn"];
            var list_GrpTable = _mdtaDtbsChat.Tables["List_Grp"];
            if (listCntnTable == null || list_GrpTable == null) return;

            UpdateUserGroup(listCntnTable, list_GrpTable, userId, groupId, members);
            var updateGroup = GetGroupUpdates(list_GrpTable);
            await NotifyGroupMembers(list_GrpTable, listCntnTable, groupId, updateGroup);
        }
        #endregion

        #region "Xử lý sự kiện thoát group"
        public async Task LeaveGroup(string userId, string groupId)
        {
            var listCntnTable = _mdtaDtbsChat.Tables["ListCntn"];
            var list_GrpTable = _mdtaDtbsChat.Tables["List_Grp"];
            if (listCntnTable == null || list_GrpTable == null) return;

            UpdateUserGroupOnLeave(listCntnTable, list_GrpTable, userId, groupId);
            var updateGroup = GetGroupUpdates(list_GrpTable);
            await NotifyGroupMembers(list_GrpTable, listCntnTable, groupId, updateGroup);
        }
        #endregion

        #region "Xử lý sự kiện gửi tin nhắn"
        public async Task SendMessage(string userId, string groupId, JArray message)
        {
            var listCntnTable = _mdtaDtbsChat.Tables["ListCntn"];
            var list_GrpTable = _mdtaDtbsChat.Tables["List_Grp"];
            if (listCntnTable == null || list_GrpTable == null) return;

            await NotifyMessage(list_GrpTable, listCntnTable, groupId, message, userId);
        }
        #endregion

        #region "Xử lý sự kiện tạo group
        public async Task CreateGroup(string userId, string memberString, JArray newGroup)
        {
            var listCntnTable = _mdtaDtbsChat.Tables["ListCntn"];
            if (listCntnTable == null) return;

            var memberIds = memberString.Split(',').ToList();
            await NotifyMembers(listCntnTable, memberIds, newGroup, userId, "newGroup");
        }
        #endregion

        #region "Xử lý sự kiện xóa group
        public async Task DeleteGroup(string userId, string groupId, string memberString)
        {
            var listCntnTable = _mdtaDtbsChat.Tables["ListCntn"];
            if (listCntnTable == null) return;

            var memberIds = memberString.Split(',').ToList();
            await NotifyMembers(listCntnTable, memberIds, groupId, userId, "removeGroup");
        }
        #endregion


        #region "Private methods"
        private void crtListUser()
        {
            DataColumn lobjUserCode;
            DataTable ltblListUser = new DataTable("ListUser");
            lobjUserCode = ltblListUser.Columns.Add("UserCode", typeof(string));
            ltblListUser.PrimaryKey = new DataColumn[] { lobjUserCode };
            _mdtaDtbsChat.Tables.Add(ltblListUser);
        }

        private void crtListCntn()
        {
            DataColumn lobjCntnCode;
            DataTable ltblListCntn = new DataTable("ListCntn");
            lobjCntnCode = ltblListCntn.Columns.Add("CntnCode", typeof(string)); ;
            ltblListCntn.Columns.Add("UserCode", typeof(string));
            ltblListCntn.Columns.Add("Grp_Code", typeof(string));
            ltblListCntn.PrimaryKey = new DataColumn[] { lobjCntnCode };
            _mdtaDtbsChat.Tables.Add(ltblListCntn);
        }

        private void crtList_Grp()
        {
            DataColumn lobjGrp_Code;
            DataTable ltblList_Grp = new DataTable("List_Grp");
            lobjGrp_Code = ltblList_Grp.Columns.Add("Grp_Code", typeof(string));
            ltblList_Grp.Columns.Add("ListJoin", typeof(string));
            ltblList_Grp.Columns.Add("ListMebr", typeof(string));
            ltblList_Grp.PrimaryKey = new DataColumn[] { lobjGrp_Code };
            _mdtaDtbsChat.Tables.Add(ltblList_Grp);
        }
        #endregion

        #region  "Thông báo danh sách users online"
        private async Task UpdateOnlineUsers(DataTable listCntnTable)
        {
            var onlineUsers = listCntnTable.Select().Select(row => new { CntnCode = row["CntnCode"], UserCode = row["UserCode"] }).ToArray();
            await Clients.All.SendAsync("UpdateOnlineUsers", onlineUsers);
        }
        #endregion

        #region  "Thêm user vào group"
        private void UpdateUserGroup(DataTable listCntnTable, DataTable list_GrpTable, string userId, string groupId, string members)
        {
            var listCntnRows = listCntnTable.Select($"UserCode = '{userId}'");
            if (listCntnRows.Length > 0)
            {
                var existingRow = listCntnRows[0];
                var existingGrp_Code = existingRow["Grp_Code"].ToString();
                if (!string.IsNullOrEmpty(existingGrp_Code))
                {
                    var list_GrpRows = list_GrpTable.Select($"Grp_Code = '{existingGrp_Code}'");
                    if (list_GrpRows.Length > 0)
                    {
                        var existingGrpRow = list_GrpRows[0];
                        var userIds = existingGrpRow["ListJoin"].ToString().Split(',').ToList();
                        userIds.Remove(userId);
                        if (userIds.Count > 0)
                        {
                            existingGrpRow["ListJoin"] = string.Join(",", userIds);
                        }
                        else
                        {
                            list_GrpTable.Rows.Remove(existingGrpRow);
                        }
                    }
                }
                existingRow["Grp_Code"] = groupId;
            }

            var grpRows = list_GrpTable.Select($"Grp_Code = '{groupId}'");
            if (grpRows.Length > 0)
            {
                var existingGrpRow = grpRows[0];
                existingGrpRow["ListJoin"] = string.IsNullOrEmpty(existingGrpRow["ListJoin"].ToString()) ? userId : $"{existingGrpRow["ListJoin"]},{userId}";
            }
            else
            {
                var newRow = list_GrpTable.NewRow();
                newRow["Grp_Code"] = groupId;
                newRow["ListJoin"] = userId;
                newRow["ListMebr"] = members;
                list_GrpTable.Rows.Add(newRow);
            }
        }
        #endregion

        #region "Xóa user trong group"
        private void UpdateUserGroupOnLeave(DataTable listCntnTable, DataTable list_GrpTable, string userId, string groupId)
        {
            var listCntnRows = listCntnTable.Select($"UserCode = '{userId}'");
            if (listCntnRows.Length > 0)
            {
                var existingRow = listCntnRows[0];
                existingRow["Grp_Code"] = string.Empty;
            }

            var list_GrpRows = list_GrpTable.Select($"Grp_Code = '{groupId}'");
            if (list_GrpRows.Length > 0)
            {
                var existingGrpRow = list_GrpRows[0];
                var userIds = existingGrpRow["ListJoin"].ToString().Split(',').ToList();
                userIds.Remove(userId);
                existingGrpRow["ListJoin"] = string.Join(",", userIds);
                if (userIds.Count == 0)
                {
                    list_GrpTable.Rows.Remove(existingGrpRow);
                }
            }
        }

        private async Task UpdateGroupOnLeave(DataTable list_GrpTable, DataTable listCntnTable, string existingGrp_Code, string existingUserCode)
        {
            var existingGrpRow = list_GrpTable.Rows.Find(existingGrp_Code);
            if (existingGrpRow != null)
            {
                var listJoin = existingGrpRow["ListJoin"].ToString();
                var userIds = listJoin.Split(',').ToList();
                userIds.Remove(existingUserCode);
                existingGrpRow["ListJoin"] = string.Join(",", userIds);
                if (userIds.Count == 0)
                {
                    list_GrpTable.Rows.Remove(existingGrpRow);
                }
                var updateGroup = GetGroupUpdates(list_GrpTable);
                await NotifyGroupMembers(list_GrpTable, listCntnTable, existingGrp_Code, updateGroup);
            }
        }
        #endregion

        #region "Lấy list group"
        private JArray GetGroupUpdates(DataTable list_GrpTable)
        {
            return JArray.FromObject(list_GrpTable.Select().Select(row => new
            {
                Grp_Code = row["Grp_Code"].ToString(),
                ListJoin = row["ListJoin"].ToString(),
                ListMebr = row["ListMebr"].ToString()
            }).ToArray());
        }
        #endregion

        #region "Gửi thông báo đến client"
        private async Task NotifyGroupMembers(DataTable list_GrpTable, DataTable listCntnTable, string groupId, JArray updateGroup)
        {
            var groupRow = list_GrpTable.Rows.Find(groupId);
            if (groupRow != null)
            {
                var userIds = groupRow["ListJoin"].ToString().Split(',').ToList();
                foreach (var userId in userIds)
                {
                    var connectionRows = listCntnTable.Select($"UserCode = '{userId}'");
                    foreach (var connectionRow in connectionRows)
                    {
                        var cntnCode = connectionRow["CntnCode"].ToString();
                        await Clients.Client(cntnCode).SendAsync("UpdateGroup", updateGroup);
                    }
                }
            }
        }

        private async Task NotifyMessage(DataTable list_GrpTable, DataTable listCntnTable, string groupId, JArray message, string userId)
        {
            var groupRow = list_GrpTable.Rows.Find(groupId);
            if (groupRow != null)
            {
                var userIds = groupRow["ListJoin"].ToString().Split(',').ToList();
                foreach (var id in userIds)
                {
                    if (id == userId) continue;
                    var connectionRows = listCntnTable.Select($"UserCode = '{id}'");
                    foreach (var connectionRow in connectionRows)
                    {
                        var cntnCode = connectionRow["CntnCode"].ToString();
                        await Clients.Client(cntnCode).SendAsync("ReceiveMessage", message, userId);
                    }
                }
            }
        }

        private async Task NotifyMembers(DataTable listCntnTable, List<string> memberIds, object content, string userId, string method)
        {
            foreach (var memberId in memberIds)
            {
                if (memberId == userId) continue;
                var connectionRows = listCntnTable.Select($"UserCode = '{memberId}'");
                foreach (var connectionRow in connectionRows)
                {
                    var cntnCode = connectionRow["CntnCode"].ToString();
                    await Clients.Client(cntnCode).SendAsync(method, content, userId);
                }
            }
        }
        #endregion
    }
}
