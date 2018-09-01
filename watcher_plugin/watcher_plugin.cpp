/**
*  @file
*  @copyright eosauthority - free to use and modify - see LICENSE.txt
*/
#include <eosio/watcher_plugin/watcher_plugin.hpp>
#include <eosio/watcher_plugin/http_async_client.hpp>
#include <eosio/chain/controller.hpp>
#include <eosio/chain/trace.hpp>
#include <eosio/chain_plugin/chain_plugin.hpp>
#include <eosio/chain/block_state.hpp>

#include <fc/io/json.hpp>
#include <fc/network/url.hpp>

#include <boost/signals2/connection.hpp>
#include <boost/algorithm/string.hpp>

#include <unordered_map>


namespace eosio {
   static appbase::abstract_plugin& _watcher_plugin = app().register_plugin<watcher_plugin>();

   using namespace chain;


   class watcher_plugin_impl {
   public:
      typedef std::unordered_multimap<transaction_id_type, action> action_queue_t;

      static const int64_t          default_age_limit = 60;
      static const fc::microseconds http_timeout;
      static const fc::microseconds max_deserialization_time;

      struct action_notif {
         action_notif(const action& act, transaction_id_type tx_id, const variant& action_data, const uint32_t block_num)
         : tx_id(tx_id), block_num(block_num), account(act.account), name(act.name), authorization(act.authorization),
         action_data(action_data) {}

         transaction_id_type      tx_id;
         uint32_t                 block_num;
         account_name             account;
         action_name              name;
         vector<permission_level> authorization;
         fc::variant              action_data;
      };

      struct message {
         std::vector<action_notif> actions;
      };

      struct filter_entry {
         name receiver;
         name action;

         std::tuple<name, name> key() const {
            return std::make_tuple(receiver, action);
         }

         friend bool operator<( const filter_entry& a, const filter_entry& b ) {
            return a.key() < b.key();
         }
      };


      chain_plugin* chain_plug = nullptr;
      fc::optional<boost::signals2::scoped_connection> accepted_block_conn;
      fc::optional<boost::signals2::scoped_connection> applied_tx_conn;
      std::set<watcher_plugin_impl::filter_entry>      filter_on;
      http_async_client                                httpc;
      fc::url                                          receiver_url;
      int64_t                                          age_limit = default_age_limit;
      action_queue_t                                   action_queue;


      bool filter( const action_trace& act ) {
         if( filter_on.find({ act.receipt.receiver, act.act.name }) != filter_on.end() )
         return true;
         else if ( filter_on.find({ act.receipt.receiver, 0 }) != filter_on.end() )
         return true;
         return false;
      }

      fc::variant deserialize_action_data(action act) {
         //~ ilog("deserialize_action_data - action:", ("u",act));  //~ happens on every block
         auto& chain = chain_plug->chain();
         auto serializer = chain.get_abi_serializer(act.account, max_deserialization_time);
         FC_ASSERT(serializer.valid() &&
         serializer->get_action_type(act.name) != action_name(),
         "Unable to get abi for account: ${acc}, action: ${a} Not sending notification.",
         ("acc", act.account)("a", act.name));
         return serializer->binary_to_variant(act.name.to_string(), act.data,
         max_deserialization_time);
      }

      void on_action_trace( const action_trace& act, const transaction_id_type& tx_id ) {
         //~ ilog("on_action_trace: ${u}", ("u",act));
         if( filter( act ) ) {
            action_queue.insert(std::make_pair(tx_id, act.act));
            //~ ilog("Added to action_queue: ${u}", ("u",act.act));
         }

         for( const auto& iline : act.inline_traces ) {
            //~ ilog("Processing inline_trace: ${u}", ("u",iline));
            on_action_trace( iline, tx_id );
         }
      }

      void on_applied_tx(const transaction_trace_ptr& trace) {
         //~ ilog("on_applied_tx - trace object: ${u}", ("u",trace));
         auto id = trace->id;
         //~ ilog("trace->id: ${u}",("u",trace->id));
         //~ ilog("action_queue.count(id): ${u}",("u",action_queue.count(id)));
         if( !action_queue.count(id) ) {
            for( auto& at : trace->action_traces ) {
               on_action_trace(at, id);
            }
         }
      }

      void build_message(message& msg, const transaction_id_type& tx_id, uint32_t block_num) {
         //~ ilog("inside build_message - block_num: ${u}", ("u",block_num));
         auto range = action_queue.equal_range(tx_id);
         for( auto& it = range.first; it != range.second; it++) {
            //~ ilog("inside build_message for loop on iterator for action_queue range");
            //~ ilog("iterator it->first: ${u}", ("u",it->first));
            //~ ilog("iterator it->second: ${u}", ("u",it->second));
            auto act_data = deserialize_action_data(it->second);
            action_notif notif( it->second, tx_id, std::forward<fc::variant>(act_data), block_num );
            //~ ilog("inside for loop for each action - action_notif: ${u}", ("u",notif));
            msg.actions.push_back(notif);
         }
      }

      void send_message(const message& msg) {
        //~ ilog("send_message msg: ${u}",("u",msg));
         //~ dlog("Sending: ${a}", ("a", fc::json::to_pretty_string(msg)));
         try{
            httpc.post(receiver_url, msg, fc::time_point::now() + http_timeout );
         }
         FC_CAPTURE_AND_LOG(("Error while sending notification")(msg));
      }

      void on_accepted_block(const block_state_ptr& block_state) {
         //~ ilog("on_accepted_block | block_state->block: ${u}", ("u",block_state->block));
         fc::time_point btime = block_state->block->timestamp;
         if( age_limit == -1 || (fc::time_point::now() - btime < fc::seconds(age_limit)) ) {
            message msg;
            transaction_id_type tx_id;
            uint32_t block_num = block_state->block->block_num();
            //~ ilog("Block_num: ${u}", ("u",block_num));

            //~ Process transactions from `block_state->block->transactions` because it includes all transactions including deferred ones
            //~ ilog("Looping over all transaction objects in block_state->block->transactions");
            for( const auto& trx : block_state->block->transactions ) {
              if( trx.trx.contains<transaction_id_type>() ) {
                //~ For deferred transactions the transaction id is easily accessible
                //~ ilog("===> block_state->block->transactions->trx ID: ${u}", ("u",trx.trx.get<transaction_id_type>()));
                tx_id = trx.trx.get<transaction_id_type>();
              }
              else {
                //~ For non-deferred transactions we have to access the txid from within the packed transaction. The `trx` structure and `id()` getter method are defined in `transaction.hpp`
                //~ ilog("===> block_state->block->transactions->trx ID: ${u}", ("u",trx.trx.get<packed_transaction>().id()));
                tx_id = trx.trx.get<packed_transaction>().id();
              }

              //~ ilog("action_queue.size: ${u}", ("u",action_queue.size()));
              if( action_queue.count(tx_id) ) {
                 build_message(msg, tx_id, block_num);
              }
            }

            //~ ilog("Done processing block_state->block->transactions");

            if( msg.actions.size() > 0 ) {
               //~ ilog("Sending message - msg.actions.size(): ${u}",("u",msg.actions.size()));
               send_message(msg);
            }
         }
         // TODO: Leave unsent actions until they are expired or are included in future blocks?
         action_queue.clear();
      }
   };

   const fc::microseconds watcher_plugin_impl::http_timeout = fc::seconds(10);
   const fc::microseconds watcher_plugin_impl::max_deserialization_time = fc::seconds(5);
   const int64_t watcher_plugin_impl::default_age_limit;

   watcher_plugin::watcher_plugin() : my(new watcher_plugin_impl()){}
   watcher_plugin::~watcher_plugin() {}

   void watcher_plugin::set_program_options(options_description&, options_description& cfg) {
      cfg.add_options()
      ("watch", bpo::value<vector<string>>()->composing(),
      "Track actions which match account:action. In case action is not specified, "
      "all actions of specified account are tracked.")
      ("watch-receiver-url", bpo::value<string>(),
      "URL where to send actions being tracked")
      ("watch-age-limit",
      bpo::value<int64_t>()->default_value(watcher_plugin_impl::default_age_limit),
      "Age limit in seconds for blocks to send notifications about."
      "No age limit if set to negative.");

   }

   void watcher_plugin::plugin_initialize(const variables_map& options) {

      try {
         EOS_ASSERT(options.count("watch-receiver-url") == 1, fc::invalid_arg_exception,
         "watch_plugin requires one watch-receiver-url to be specified!");

         string url_str = options.at("watch-receiver-url").as<string>();
         my->receiver_url = fc::url(url_str);

         if (options.count("watch")) {
            auto fo = options.at("watch").as<vector<string>>();
            for (auto& s : fo) {
               // TODO: Don't require ':' for watching whole accounts
               std::vector<std::string> v;
               boost::split(v, s, boost::is_any_of(":"));
               EOS_ASSERT(v.size() == 2, fc::invalid_arg_exception,
               "Invalid value ${s} for --watch",
               ("s", s));
               watcher_plugin_impl::filter_entry fe{v[0], v[1]};
               EOS_ASSERT(fe.receiver.value, fc::invalid_arg_exception, "Invalid value ${s} for "
               "--watch", ("s", s));
               my->filter_on.insert(fe);
            }
         }

         if (options.count("watch-age-limit"))
         my->age_limit = options.at("watch-age-limit").as<int64_t>();


         my->chain_plug = app().find_plugin<chain_plugin>();
         auto& chain = my->chain_plug->chain();
         my->accepted_block_conn.emplace(chain.accepted_block.connect(
            [&](const block_state_ptr& b_state) {
               my->on_accepted_block(b_state);
         }));

         my->applied_tx_conn.emplace(chain.applied_transaction.connect(
            [&](const transaction_trace_ptr& tt) {
               my->on_applied_tx(tt);
            }
         ));
      } FC_LOG_AND_RETHROW()
   }

   void watcher_plugin::plugin_startup() {
      my->httpc.start();
      ilog("Watcher plugin started");
   }

   void watcher_plugin::plugin_shutdown() {
      my->applied_tx_conn.reset();
      my->accepted_block_conn.reset();
      my->httpc.stop();
   }

}

FC_REFLECT(eosio::watcher_plugin_impl::action_notif, (tx_id)(block_num)(account)(name)(authorization)
   (action_data))
FC_REFLECT(eosio::watcher_plugin_impl::message, (actions))
