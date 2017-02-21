#pragma once

#include "rhafx.h"
#include "jyafx.h"
#include "jytrade_msg.h"

struct app_gw_conf {
    std::string pos_server;
    std::string trade_gw;
    std::string trade_server;

    std::string post_addr;
    uint16_t post_port;
    std::string post_page;
};

struct client_http_req {
    E15_Id id;
    JYMSG msg;
    bool response;
};

class app_gw;
class data_trans : public E15_Server {
public:
    data_trans(app_gw *gw)
            :m_gw(gw)
            ,m_writer(m_buffer) {
        m_doc.SetObject();
    }
    virtual ~data_trans() {}

    void start();
    void stop();

public:
    /* server callback */
    virtual int OnOpen(E15_ServerInfo * info,E15_String *& json);
    virtual int OnClose(E15_ServerInfo * info);
    virtual void OnRequest(E15_ServerInfo * info,E15_ServerRoute * rt,E15_ServerCmd * cmd,E15_String *& data);
    virtual void OnResponse(E15_ServerInfo * info,E15_ServerRoute * rt,E15_ServerCmd * cmd,E15_String *& data);
    virtual void OnNotify(E15_ServerInfo * info,E15_ServerRoute * rt,E15_ServerCmd * cmd,E15_String *& data);

    void query_load(const char *uid);
    static void http_transmit(int conn, int status, std::map<std::string, std::string>& headers, std::string& body, void *args);

private:
    void post_phpserver(bool response, E15_ServerInfo *info, int cmd, const char *data, int len);

private:
    //servers
    app_gw *m_gw;
    E15_Id m_trade_gw, m_pos_server;

    rapidjson::Document m_doc;
    rapidjson::StringBuffer m_buffer;
    rapidjson::Writer<rapidjson::StringBuffer> m_writer;

    //转发http请求和响应的设施
    crx::http_client *m_http_client;
    crx::epoll_thread m_http_th;
    std::map<int, client_http_req> m_conn_session;			//it->first: 标识一次http连接, it->second: 客户端的session
    std::map<std::string, std::string> m_ext_headers;
};

struct login_session {
    E15_Id trader_id;
    std::list<E15_Id> client_ids;
};

struct login_cache {
    E15_Id trader_id;
    std::list<E15_Id> client_ids;
    E15_String *data;

    virtual ~login_cache() {
        delete data;				//删除登陆缓存json串
    }
};

class app_gw : public crx::console {
public:
    app_gw()
            :m_writer(m_buffer) {
        m_doc.SetObject();
        m_cmd_keys[MSG_LOGIN_FUTUREACC] = {"broker_id", "account", "passwd"};		//登陆
        m_cmd_keys[MSG_COMMIT_ORDER]    = {"ins_id", "md_date", "md_time", "price", "volume",
                                           "offset_flag", "direction", "vip_uid"};	//下单
    }
    virtual ~app_gw() {}

    virtual bool init(int argc, char *argv[]);
    virtual void destroy();

    void notify_trader_offline(const char *name);
    void show_login_uid();
    void respone_query_load(E15_Id& id, const std::string& uid);
    void response_login_account(int err_id, const char *uid, E15_Id& id,
                                const char *name, const std::string& json);

    void trans_result(bool response, E15_ServerCmd *cmd, E15_String *data);
    void trade_relate(E15_ServerInfo *info, E15_ServerCmd *cmd, E15_String *& data);

private:
    void cons_response_json(E15_ServerInfo *info, E15_ServerCmd *cmd, int err_id, const char *err_msg);
    bool parse_request(int cmd, const char *json, std::map<std::string, std::string>& kvs);
    static void handle_select_trader(const std::string& signal, void *args);

private:
    rapidjson::Document m_parse_doc;
    std::shared_ptr<data_trans> m_trans;
    std::map<int, std::vector<std::string>> m_cmd_keys;

    crx::event *m_load_event;
    std::mutex m_trader_mtx;
    std::map<std::string, std::set<std::string>> m_trader_uids;     //it->first: trader_name, it->second: uids
    std::map<std::string, login_session> m_uid_session;		//登陆session,it->first: uid
    std::map<std::string, login_cache> m_uid_cache;			//登陆缓存,需要首先查可用的trade_server, it->first: uid

    rapidjson::Document m_doc;			//构造响应的json串
    rapidjson::StringBuffer m_buffer;
    rapidjson::Writer<rapidjson::StringBuffer> m_writer;
};