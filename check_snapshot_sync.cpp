
auto checkSnapshotSync() {
    if (snapshot_queued_msgs.empty()) return;

    const auto& first_message = snapshot_queued_msgs.begin()->second;

    if (first_message.type_ != Exchange::MarketUpdateType::SNAPSHOT_START){

        snapshot_queued_msgs.clear();

        return;

    }

    
    size_t next_exp_seq_number = 0;
    bool have_complete_snapshot = true;

    vector<Exchange::MEMarketUpdate> final_vector;

    for (const auto& message : snapshot_queued_msgs){

        const auto& seq_number = message.first;

        if (seq_number != next_exp_seq_number) {

            have_complete_snapshot = false;
            break;

        }

        if (message.second.type_ == Exchange::MarketUpdateType::SNAPSHOT_START || message.second.type_ == Exchange::MarketUpdateType::SNAPSHOT_END) continue;
        
    

        next_exp_seq_number++;

        final_vector.push_back(message.second);

    }

    if (!have_complete_snapshot) {
        snapshot_queued_msgs.clear();
        return;
    }

    const auto& last_msg = snapshot_queued_msgs_.rbegin()->second;
    bool have_incremental = true;
    if (last_msg.type_ != Exchange::MarketUpdateType::SNAPSHOT_END) return;

    next_exp_inc_seq_number = last_msg.order_id_ + 1;


    for (const auto& message : incremental_queued_msgs_){
        
        const auto& seq_number = message.first;

        if (seq_number < next_exp_inc_seq_number) continue;

        if (seq_number != next_exp_inc_number) {
            have_incremental = false;
            break;
        }

        if (message.second.type_ == Exchange::MarketUpdateType::SNAPSHOT_START || message.second.type_ == Exchange::MarketUpdateType::SNAPSHOT_END) continue;
        
        next_exp_inc_seq_number++;

        final_vector.push_back(message.second);
    }

    if (!have_incremental){
        snapshot_queued_msgs.clear();
        return;
    }

    for (const auto& message : final_vector){
        auto next_write = incoming_md_updates->getNextToWriteTo();
        *next_write = message;
        incoming_md_updates->updateWriteIndex();
    }

    snapshot_queued_msgs_clear();
    incremental_queued_msgs_.clear();
    in_recovery  = false;
    snapshot_mcast_socket_.leave(snapshot_ip_,snapshot_port_);


}