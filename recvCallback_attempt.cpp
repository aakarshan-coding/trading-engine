
auto recvCallbackTime(TCPSocket* socket, Nanos rx_time){

    //check if there are enough bytes in the socket buffer to consider it a client request
    if (socket->next_rcv_valid_index >= sizeof(OMClientRequest)) {

        //loop through all bytes, going in chunks of size OMClientRequest
        size_t i = 0;
        for (; i < socket->next_rcv_valid_index_; i += sizeof(OMClientRequest)){
            auto client_request = reinterpret_cast<const OMClientRequest*>(socket->inbound_data_.data() + i);

            if (UNLIKELY(cid_tcp_socket_[client_request->me_client_request.client_id] == nullptr)){
                cid_tcp_socket_[client_request->me_client_request_.client_id_] = socket;
            }
            //if the message claims to be from a certain client, but is on a different socket than what the system has that client on, ignore the message
            if (UNLIKELY(cid_tcp_socket_[client_request->me_client_request_.client_id_] != socket)) continue;

            if (UNLIKELY(client_request->seq_num_ != cid_next_exp_seq_num_[client_request->me_client_request_.client_id_])) continue;

            cid_next_exp_seq_num_[client_request->me_client_request_.client_id_] += 1;

            fifo_sequencer_.addClientRequest(rx_time, client_request->me_client_request_);
        }


        //if there is any message that isn't a full OMClientRequest at the end (partial message), move it to the front of the buffer (socket->next_rcv_valid_index - i gets the size and socket->inbound_data.data() + i is starting point)
        memcpy(socket->inbound_data_.data(), socket->inbound_data_.data() + i, socket->next_rcv_valid_index - i);
        socket->next_rcv_valid_index -= i;



    }




}