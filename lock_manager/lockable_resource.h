#ifndef LOCKABLE_RESOURCE_H
#define LOCKABLE_RESOURCE_H

#include<iostream>

// A resource is locked in a 'mode' by a 'transaction'. 
enum lockType{ SHARED, EXCLUSIVE };

// The lock request may be granted or put on wait based on a lock compatibility matrix. 
enum lockStatus{ GRANTED, WAITING };

class lockable_resource{ 
    private:
        int  txn_id_;
        lockType lock_type_;        // SHARED, EXCLUSIVE
        lockStatus lock_status_;    // GRANTED, WAITING
        
    public:
        lockable_resource(int txn_id_, lockType lock_type_, lockStatus lock_status_){
            this->txn_id_ = txn_id_;
            this->lock_type_ = lock_type_;
            this->lock_status_ = lock_status_;
        }
        
        // Getters and Setters
        lockType getLockType(){
            return(lock_type_);
        }
        lockStatus getLockStatus(){
            return(lock_status_);
        }
        int getTxnId(){
            return(txn_id_);
        }
        void setLockStatus(lockStatus st){
            lock_status_ = st;
        }
        void setLockType(lockType lt){
            lock_type_ = lt;
        }
};

#endif