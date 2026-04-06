struct RecvTimeClientRequest {
    Nanos recv_time = 0;
    MEClientRequest request_;
}

class FifoSequencer {

    public:
        
        FifoSequencer(ClientRequestLFQueue* client_requests, Logger* logger) : incoming_client_requests(client_requests), logger_(logger){
        }

        void addClientRequest(Nanos receive_time, const MEClientRequest& client_request){
            if (pending_size_ >= pending_client_requests.size()) FATAL("Too many requests");

            pending_client_requests_.at(pending_size) = RecvClientRequest{receive_time, client_request};
            pending_size_ += 1;
        }

        void sequenceAndPublish(){

            std::sort(pending_clients_requests_.begin(), pending_client_requests.begin() + pending_size_);

            for (size_t i = 0; i < pending_size_;i++){

                auto next_spot = incoming_requests->getNextToWriteTo();

                *next_spot = pending_client_requests_.at(i).request_;

                incoming_requests->updateWriteIndex();
            }
            
            pending_size_ = 0;
            

        }

        //queue for handling incoming client requests
        ClientRequestLFQueue* incoming_client_requests = nullptr;

        Logger* logger_ = nullptr;

        struct RecvTimeClientRequest {
            Nanos recv_time_ = 0;
            MEClientRequest request_;

            auto operator<(const REcvTimeClientRequest& other) const {
                return this->recv_time_ < other.recv_time_;
            }
        }
        
        //keep an array of the client requests, and track the size 
        std::array<RecvTimeClientRequest, 1024> pending_client_requests_;
        size_t pending_size_ = 0;



}