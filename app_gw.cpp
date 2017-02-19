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
    }
    return 1000;		//自动重联
}

//此处必须解析客户端发送的json串，因为uid保存在串中，再根据uid来判断之前是否已经登陆过(交易实例将一直维持到OnFrontDisconnected)
void data_trans::OnRequest(E15_ServerInfo * info,E15_ServerRoute * rt,E15_ServerCmd * cmd,E15_String *& data) {
    switch (cmd->cmd) {
        case MSG_LOGIN_FUTUREACC:
        case MSG_COMMIT_ORDER: {		//将指定请求发送给域内部服务
            m_gw->trade_relate(info, cmd, data);
            break;
        }
        default: {		//其他消息都转发给php服务
            int conn = m_http_client->connect(g_conf.post_addr.c_str(), g_conf.post_port);
            if (-1 == conn) {
                g_logger.print_ts("[OnRequest] 连接php服务器失败\n");
                return;
            }

            m_conn_session[conn].id = info->id;
            m_conn_session[conn].msg = (JYMSG)cmd->cmd;

            std::map<std::string, std::string> ext_headers;
            ext_headers["Uid"] = info->name;
            m_http_client->POST(conn, g_conf.post_page.c_str(), nullptr, data->c_str(), data->Length());
            g_logger.print_ts("[name=%s %x:%x conn=%d] 收到请求 %d: %s\n", info->name, info->id.h, info->id.l,
                              conn, cmd->cmd, data->c_str());
            break;
        }
    }
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
                m_gw->response_login_account(ri->err_id, ri->uid, info->id, result);
                break;
            }
            case MSG_COMMIT_ORDER: {		//转发报单结果
                Response(&cmd->receiver, 0, cmd, data);
                break;
            }
        }
    } else if (!strcmp(info->role, g_conf.pos_server.c_str())) {
        Response(&cmd->receiver, 0, cmd, data);			//网关只做内部响应服务的转发，响应对端由cmd中的receiver字段指定
    }
}

void data_trans::http_transmit(int conn, int status, std::map<std::string, std::string>& headers,
                               std::string& body, void *args) {
    data_trans *trans = static_cast<data_trans*>(args);
    if (trans->m_conn_session.end() != trans->m_conn_session.find(conn)) {
        auto& client_session = trans->m_conn_session[conn];
        E15_ServerCmd cmd;
        cmd.cmd = client_session.msg;
        trans->Response(&client_session.id, 0, &cmd, body.c_str(), body.size());
        g_logger.print_ts("[%x:%x conn=%d status=%d] 转发http响应：%d\n", client_session.id.h, client_session.id.l,
                          conn, status, body.size());
        trans->m_conn_session.erase(conn);
    }
    trans->m_http_client->release(conn);
}

void data_trans::OnNotify(E15_ServerInfo * info,E15_ServerRoute * rt,E15_ServerCmd * cmd,E15_String *& data) {
    if (!strcmp(info->role, g_conf.trade_server.c_str())) {		//交易服务推送的消息
//		if (MSG_REPORT_TRADELOAD == cmd->cmd) {		//报告交易负载
//			int *load = (int*)data->c_str();
//			m_trade_servers[info->name].load = *load;
//			g_logger.print_ts("[OnNotify] 交易服务 %s 报告当前的负载 %d\n", info->name, *load);
//		} else {		//推送给客户端的消息
//			Notify(&cmd->receiver, 0, cmd, data);
//		}
    } else {		//其他服务推送的消息
        Notify(&cmd->receiver, 0, cmd, data);
    }
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
                    return;
                }

                auto& cache = m_uid_cache[info->name];
                cache.client_ids.push_back(info->id);
                cache.data = data;
                data = nullptr;
                m_trans->query_load(info->name);
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
                cmd->sender = info->id;
                m_trans->Request(&m_uid_session[info->name].trader_id, 0, cmd, (const char*)&order, sizeof(order));
            }
            break;
        }
    }
}

void app_gw::response_login_account(int err_id, const char *uid, E15_Id& id, const std::string& json) {
    std::lock_guard<std::mutex> lck(m_trader_mtx);
    if (m_uid_cache.end() == m_uid_cache.find(uid))
        return;

    if (!err_id) {
        m_uid_session[uid].trader_id = id;
        m_uid_session[uid].client_ids = std::move(m_uid_cache[uid].client_ids);
    }

    E15_ServerCmd cmd;
    cmd.cmd = MSG_LOGIN_FUTUREACC;
    for (auto& client_id : m_uid_session[uid].client_ids)
        m_trans->Response(&client_id, 0, &cmd, json.c_str(), json.size());
    m_uid_cache.erase(uid);
}

void app_gw::respone_query_load(E15_Id& id, const std::string& uid) {
    std::lock_guard<std::mutex> lck(m_trader_mtx);
    if (m_uid_cache.end() == m_uid_cache.find(uid))
        return;

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
}

int main(int argc, char *argv[]) {
    app_gw gw;
    return gw.run(argc, argv);
}
