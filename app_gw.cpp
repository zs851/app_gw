#include "app_gw.h"

E15_Socket g_socket;
app_gw_conf g_conf;
log_factory g_logger;

void data_trans::start() {
    m_http_th.start();
    m_http_client = m_http_th.get_http_client(http_transmit, this);
    g_socket.Start();
    Start(&g_socket, "ini/server.ini");
}

void data_trans::stop() {
    Stop();
    g_socket.Stop();
    m_http_th.stop();
}

int data_trans::OnOpen(E15_ServerInfo * info,E15_String *& json) {
    g_logger.print_ts("[%x:%x] (N=%d,name=%s:role=%s) 上线\n", info->id.h,
                      info->id.l, info->N, info->name, info->role);
    if (info->N) {			//servers
        if (!strcmp(info->role, g_conf.pos_server.c_str()))
            m_pos_server = info->id;
        else if (!strcmp(info->role, g_conf.trade_gw.c_str()))
            m_trade_gw = info->id;
        else if (!strcmp(info->role, g_conf.trade_server.c_str()))
            m_gw->notify_trader_offline(info->name);
    } else {
        rapidjson::Document::AllocatorType& alloc = m_doc.GetAllocator();
        m_doc.AddMember("action", "loginMsg", alloc);
        m_doc.AddMember("uid", rapidjson::Value().SetString(info->name, strlen(info->name)), alloc);

        m_doc.Accept(m_writer);
        std::string json = m_buffer.GetString();
        m_doc.RemoveAllMembers();
        m_buffer.Clear();
        m_writer.Reset(m_buffer);
        post_phpserver(false, info, MSG_NOTIFY_USRLOGIN, json.c_str(), json.size());
        g_logger.print_ts("[OnOpen] 通知php后台用户%s(%x:%x)上线 %s\n", info->name,
                          info->id.h, info->id.l, json.c_str());
    }
    return 1;
}

int data_trans::OnClose(E15_ServerInfo * info) {
    g_logger.print_ts("[%x:%x] (N=%d, name=%s:role=%s) 下线\n", info->id.h, info->id.l,
                      info->N, info->name, info->role);
    if (info->N) {			//servers
        if (!strcmp(info->role, g_conf.pos_server.c_str()))
            m_pos_server.Reset();
        else if (!strcmp(info->role, g_conf.trade_gw.c_str()))
            m_trade_gw.Reset();
    } else {
        rapidjson::Document::AllocatorType& alloc = m_doc.GetAllocator();
        m_doc.AddMember("action", "logoutMsg", alloc);
        m_doc.AddMember("uid", rapidjson::Value().SetString(info->name, strlen(info->name)), alloc);

        m_doc.Accept(m_writer);
        std::string json = m_buffer.GetString();
        m_doc.RemoveAllMembers();
        m_buffer.Clear();
        m_writer.Reset(m_buffer);
        post_phpserver(false, info, MSG_NOTIFY_USRLOGOUT, json.c_str(), json.size());
        g_logger.print_ts("[OnClose] 通知php后台用户%s(%x:%x)下线 %s\n", info->name,
                          info->id.h, info->id.l, json.c_str());
    }
    return 1000;		//自动重联
}

//此处必须解析客户端发送的json串，因为uid保存在串中，再根据uid来判断之前是否已经登陆过(交易实例将一直维持到OnFrontDisconnected)
void data_trans::OnRequest(E15_ServerInfo * info,E15_ServerRoute * rt,E15_ServerCmd * cmd,E15_String *& data) {
    g_logger.print_ts("[name=%s %x:%x] 收到请求 %d: %s\n", info->name,
                      info->id.h, info->id.l, cmd->cmd, data->c_str());

    switch (cmd->cmd) {
        case MSG_LOGIN_FUTUREACC:
        case MSG_COMMIT_ORDER:
        case MSG_ACTIVATE_LEADERORDER: {		//将指定请求发送给域内部服务
            m_gw->trade_relate(info, cmd, data);
            break;
        }
        default: {		//其他消息都转发给php服务
            post_phpserver(true, info, cmd->cmd, data->c_str(), data->Length());
            break;
        }
    }
}

void data_trans::post_phpserver(bool response, E15_ServerInfo *info,
                                int cmd, const char *data, int len) {
    int conn = m_http_client->connect(g_conf.post_addr.c_str(), g_conf.post_port);
    if (-1 == conn) {
        g_logger.print_ts("[OnRequest] 连接php服务器失败\n");
        return;
    }

    m_conn_session[conn].id = info->id;
    m_conn_session[conn].msg = (JYMSG)cmd;
    m_conn_session[conn].response = response;

    m_ext_headers["Uid"] = info->name;
    m_http_client->POST(conn, g_conf.post_page.c_str(), &m_ext_headers, data, len);
}

void data_trans::http_transmit(int conn, int status, std::map<std::string, std::string>& headers,
                               std::string& body, void *args) {
    data_trans *trans = static_cast<data_trans*>(args);
    if (trans->m_conn_session.end() != trans->m_conn_session.find(conn)) {
        auto& client_session = trans->m_conn_session[conn];
        E15_ServerCmd cmd;
        cmd.cmd = client_session.msg;
        if (client_session.response)        //响应
            trans->Response(&client_session.id, 0, &cmd, body.c_str(), body.size());
        else
            trans->Notify(&client_session.id, 0, &cmd, body.c_str(), body.size());
        g_logger.print_ts("[%x:%x conn=%d status=%d] 转发http响应：%d\n", client_session.id.h, client_session.id.l,
                          conn, status, body.size());
        trans->m_conn_session.erase(conn);
    }
    trans->m_http_client->release(conn);
}

void data_trans::query_load(const char *uid) {
    E15_ServerCmd cmd;
    cmd.cmd = MSG_QUERY_LEASTLOAD;
    Request(&m_trade_gw, 0, &cmd, uid, strlen(uid));
}

void data_trans::OnResponse(E15_ServerInfo * info,E15_ServerRoute * rt,E15_ServerCmd * cmd,E15_String *& data) {
    if (!strcmp(info->role, g_conf.trade_gw.c_str())) {
        if (MSG_QUERY_LEASTLOAD == cmd->cmd)
            m_gw->respone_query_load(cmd->receiver, data->c_str());
    } else if (!strcmp(info->role, g_conf.trade_server.c_str())) {
        switch (cmd->cmd) {
            case MSG_LOGIN_FUTUREACC: {
                rsp_info *ri = (rsp_info*)data->c_str();
                rapidjson::Document::AllocatorType& alloc = m_doc.GetAllocator();
                m_doc.AddMember("err_id", rapidjson::Value().SetInt(ri->err_id), alloc);
                if (ri->err_id)
                    m_doc.AddMember("err_msg", rapidjson::Value().SetString(ri->err_msg, strlen(ri->err_msg)), alloc);

                std::string result = m_doc.GetString();
                m_doc.RemoveAllMembers();
                m_buffer.Clear();
                m_writer.Reset(m_buffer);
                m_gw->response_login_account(ri->err_id, ri->uid, info->id, info->name, result);
                break;
            }
            case MSG_COMMIT_ORDER: {		//转发报单结果
                m_gw->trans_result(true, cmd, data);
                break;
            }
        }
    } else if (!strcmp(info->role, g_conf.pos_server.c_str())) {
        Response(&cmd->receiver, 0, cmd, data);			//网关只做内部响应服务的转发，响应对端由cmd中的receiver字段指定
    }
}

void data_trans::OnNotify(E15_ServerInfo * info,E15_ServerRoute * rt,E15_ServerCmd * cmd,E15_String *& data) {
    m_gw->trans_result(false, cmd, data);     //转发其他服务推送的消息
}

bool app_gw::init(int argc, char *argv[]) {
    g_logger.init("app_gw");
    E15_Ini ini;
    ini.Read("ini/config.ini");
    ini.SetSection("server_role");
    g_conf.pos_server = ini.ReadString("pos_server", "");
    g_conf.trade_gw = ini.ReadString("trade_gw", "");
    g_conf.trade_server = ini.ReadString("trade_server", "");

    ini.SetSection("php_backend");
    g_conf.post_addr = ini.ReadString("post_addr", "");
    ini.Read("post_port", g_conf.post_port);
    g_conf.post_page = ini.ReadString("post_page", "");

    //启动基础通信设施
    m_load_event = get_event(handle_select_trader, this);
    m_trans = std::make_shared<data_trans>(this);
    m_trans->start();
    return true;
}

void app_gw::destroy() {
    m_trans->stop();
    m_trans.reset();
    m_load_event->release();
    g_logger.destroy();
}

void app_gw::cons_response_json(E15_ServerInfo *info, E15_ServerCmd *cmd,
                                int err_id, const char *err_msg) {
    rapidjson::Document::AllocatorType& alloc = m_doc.GetAllocator();
    m_doc.AddMember("err_id", rapidjson::Value().SetInt(0), alloc);
    if (err_msg)
        m_doc.AddMember("err_msg", rapidjson::Value().SetString(err_msg, strlen(err_msg)), alloc);
    m_doc.Accept(m_writer);
    std::string json = m_buffer.GetString();

    m_doc.RemoveAllMembers();
    m_buffer.Clear();
    m_writer.Reset(m_buffer);
    m_trans->Response(&info->id, 0, cmd, json.c_str(), json.size());
    g_logger.print_ts("[cons_response_json] name=%s,cmd=%d 返回响应 %s\n",
                      info->name, cmd->cmd, json.c_str());
}

bool app_gw::parse_request(int cmd, const char *json, std::map<std::string, std::string>& kvs) {
    g_logger.print_ts("[parse_request] 收到app_client发送的请求：%s\n", json);
    if (m_parse_doc.ParseInsitu((char*)json).HasParseError()) {		//解析出错
        g_logger.print_ts("[OnRequest] 无效的json串\n");
        return false;
    }

    for (auto& key : m_cmd_keys[cmd]) {
        if (!m_parse_doc.HasMember(key.c_str()) || !m_parse_doc[key.c_str()].IsString()) {
            g_logger.print_ts("[parse_request] json串中不存在key为%s的键值对或者该键"
                                      "对应的值不是字符串，拒绝本次交易请求！\n", key.c_str());
            return false;
        }
        kvs[key] = m_parse_doc[key.c_str()].GetString();
    }
    return true;
}

void app_gw::trade_relate(E15_ServerInfo *info, E15_ServerCmd *cmd, E15_String *& data) {
    std::lock_guard<std::mutex> lck(m_trader_mtx);
    switch (cmd->cmd) {
        case MSG_LOGIN_FUTUREACC: {		//登陆
            if (m_uid_session.end() == m_uid_session.find(info->name)) {		//之前还未登陆，发送登陆请求
                if (m_uid_cache.end() != m_uid_cache.find(info->name)) {		//该账号的其他的设备发起登陆请求
                    auto& cache = m_uid_cache[info->name];
                    cache.client_ids.push_back(info->id);
                    g_logger.print_ts("[trade_relate]未登陆时同时有多个账号发起登陆请求,记录当前连接"
                                              "%s(%x:%x)\n", info->name, info->id.h, info->id.l);
                    return;
                }

                auto& cache = m_uid_cache[info->name];
                cache.client_ids.push_back(info->id);
                cache.data = data;
                data = nullptr;
                m_trans->query_load(info->name);
                g_logger.print_ts("[trade_relate] 用户%s(%x:%x)未登陆,查询负载\n", info->name,
                                  info->id.h, info->id.l);
            } else {		//通知已登陆完成消息
                cons_response_json(info, cmd, 0, nullptr);
            }
            break;
        }

        case MSG_COMMIT_ORDER: {			//下单
            if (m_uid_session.end() == m_uid_session.find(info->name)) {		//还未登陆，报告异常消息
                cons_response_json(info, cmd, 1, "用户还未登陆");
            } else {
                std::map<std::string, std::string> kvs;
                if (!parse_request(cmd->cmd, data->c_str(), kvs))
                    return;

                commit_order order;
                bzero(&order, sizeof(order));
                strcpy(order.uid, info->name);
                strcpy(order.ins_id, kvs["ins_id"].c_str());
                order.volume = std::atoi(kvs["volume"].c_str());
                order.offset_flag = std::atoi(kvs["offset_flag"].c_str());
                order.direction = std::atoi(kvs["direction"].c_str());
                strcpy(order.vip_uid, kvs["vip_uid"].c_str());
                m_trans->Request(&m_uid_session[info->name].trader_id, 0, cmd, (const char*)&order, sizeof(order));
                g_logger.print_ts("[trade_relate] 用户%s(%x:%x)提交下单请求: ins_id=%s,volume=%d,"
                                          "offset_flag=%d, direction=%d\n", info->name, info->id.h, info->id.l,
                                  order.ins_id, order.volume, order.offset_flag, order.direction);
            }
            break;
        }

        case MSG_ACTIVATE_LEADERORDER: {        //激活领单
            if (m_uid_session.end() == m_uid_session.find(info->name)) {        //还未登陆
                cons_response_json(info, cmd, 1, "用户还未登陆");
            } else {
                E15_String str;
                str.Memcpy(info->name, strlen(info->name));
                str.Resize(16, 0);
                str.Memcat(data->c_str(), data->Length());
                m_trans->Request(&m_uid_session[info->name].trader_id, 0, cmd, str.c_str(), str.Length());
                g_logger.print_ts("[trade_relate] 用户%s(%x:%x)发起激活领单: json=%s\n", info->name,
                                  info->id.h, info->id.l, data->c_str());
            }
            break;
        }
    }
}

void app_gw::response_login_account(int err_id, const char *uid, E15_Id& id,
                                    const char *name, const std::string& json) {
    std::lock_guard<std::mutex> lck(m_trader_mtx);
    if (m_uid_cache.end() == m_uid_cache.find(uid))
        return;

    if (!err_id) {
        m_uid_session[uid].trader_id = id;
        m_uid_session[uid].client_ids = std::move(m_uid_cache[uid].client_ids);
        m_trader_uids[name].insert(uid);
    }

    E15_ServerCmd cmd;
    cmd.cmd = MSG_LOGIN_FUTUREACC;
    for (auto& client_id : m_uid_session[uid].client_ids)
        m_trans->Response(&client_id, 0, &cmd, json.c_str(), json.size());
    m_uid_cache.erase(uid);
    g_logger.print_ts("[response_login_account] 用户%s在交易服务%x:%x上的登陆请求反馈的结果: %s\n",
                      uid, id.h, id.l, json.c_str());
}

void app_gw::trans_result(bool response, E15_ServerCmd *cmd, E15_String *data) {
    const char *uid = data->c_str();
    std::lock_guard<std::mutex> lck(m_trader_mtx);
    if (m_uid_session.end() == m_uid_session.find(uid))
        return;

    auto& session = m_uid_session[uid];
    for (auto& client_id : session.client_ids) {
        if (response)
            m_trans->Response(&client_id, 0, cmd, data->c_str()+16, data->Length()-16);
        else
            m_trans->Notify(&client_id, 0, cmd, data->c_str()+16, data->Length()-16);
    }
    g_logger.print_ts("[trans_result] 转发用户(%s)的响应或推送: %s\n", uid, data->c_str()+16);
}

void app_gw::notify_trader_offline(const char *name) {
    std::lock_guard<std::mutex> lck(m_trader_mtx);
    if (m_trader_uids.end() == m_trader_uids.find(name))
        return;

    for (auto& uid : m_trader_uids[name])
        m_uid_session.erase(uid);
    m_trader_uids.erase(name);
}

void app_gw::respone_query_load(E15_Id& id, const std::string& uid) {
    std::lock_guard<std::mutex> lck(m_trader_mtx);
    if (m_uid_cache.end() == m_uid_cache.find(uid))
        return;

    g_logger.print_ts("[respone_query_load] 得到查询负载的响应,用户(%s)将在交易服务%x:%x执行登陆操作\n",
                      uid.c_str(), id.h, id.l);
    auto& cache = m_uid_cache[uid];
    cache.trader_id = id;
    m_load_event->send_signal(uid.c_str(), uid.size());
}

void app_gw::handle_select_trader(const std::string& signal, void *args) {
    app_gw *gw = static_cast<app_gw*>(args);
    std::lock_guard<std::mutex> lck(gw->m_trader_mtx);
    if (gw->m_uid_cache.end() == gw->m_uid_cache.find(signal))
        return;

    auto& cache = gw->m_uid_cache[signal];
    std::map<std::string, std::string> kvs;
    gw->parse_request(MSG_LOGIN_FUTUREACC, cache.data->c_str(), kvs);

    trader_login login;
    bzero(&login, sizeof(login));
    strcpy(login.uid, signal.c_str());
    strcpy(login.broker_id, kvs["broker_id"].c_str());
    strcpy(login.account, kvs["account"].c_str());
    strcpy(login.passwd, kvs["passwd"].c_str());

    E15_ServerCmd cmd;
    cmd.cmd = MSG_LOGIN_FUTUREACC;
    gw->m_trans->Request(&cache.trader_id, 0, &cmd, (const char*)&login, sizeof(login));
    g_logger.print_ts("[handle_select_trader] 向交易服务%x:%x 发送用户登陆请求: uid=%s, broker_id=%s, "
                              "account=%s, password=%s\n", cache.trader_id.h, cache.trader_id.l,
                      login.uid, login.broker_id, login.account, login.passwd);
}

void app_gw::show_login_uid() {
    int index = 1;
    std::stringstream ss;
    ss<<'\n'<<std::left<<std::setw(8)<<"seq"<<std::setw(16)<<"uid"<<"trade_server\n";
    char buffer[32];
    std::lock_guard<std::mutex> lck(m_trader_mtx);
    for (auto& us : m_uid_session) {
        sprintf(buffer, "%x:%x", us.second.trader_id.h, us.second.trader_id.l);
        ss<<std::left<<std::setw(8)<<index++<<std::setw(16)<<us.first<<buffer<<'\n';
    }
    std::cout<<ss.rdbuf()<<std::endl;
}

int main(int argc, char *argv[]) {
    app_gw gw;
    gw.add_cmd("su", [&](const std::vector<std::string>& args, crx::console *c){
        gw.show_login_uid();
    }, "显示当前处于交易状态的用户");
    return gw.run(argc, argv);
}