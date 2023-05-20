/*
MTCS-203(P): Topics in Database Management Systems
Summative Test

By: Shiva Krishna
I M.Tech.(CS)
Regd no: 22555
*/

/* 
Build a lock manager. 
The lock manager should support the following capabilities:
    1. Lock a resource in either shared or exclusive mode. 
    2. Unlock a resource held by a transaction. 
A resource will be identified by a 10 character string. 
*/

/*
Implement Upgrade and Downgrade
*/

#include <iostream>
#include <unordered_map>
#include <list>

using namespace std;

// Abstraction of a resource that can be locked. 
// A resource is locked in a 'mode' by a 'transaction'. 
// The lock request may be granted or put on wait based on a lock compatibility matrix. 
enum lockType{
  SHARED,
  EXCLUSIVE
};

enum lockStatus{
  GRANTED,
  WAITING
};

class lockable_resource{ 
    private:
        int  txn_id_;
        lockType lock_type_; // SHARED, EXCLUSIVE
        lockStatus lock_status_; // GRANTED, WAITING
        
    public:
        lockable_resource(int txn_id, lockType lock_type, lockStatus lock_status):txn_id_(txn_id), lock_type_(lock_type), lock_status_(lock_status){}
        
        lockType getLockType(){
            return(lock_type_);
        }
        lockStatus getStatus(){
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

// Resource
// type of lock
// txn_id

void lock(std::string resource_name, int txn_id, lockType lock_type); // Lock function prototype
void unlock(std::string resource_name, int txn_id);     // Unlock function prototype
void printStatus(std::string resource_name);            // Print function prototype
void upgrade(std::string resource_name, int txn_id);    // upgrade function prototype
void downgrade(std::string resource_name, int txn_id);  // downgrade function prototype

unordered_map< std::string, list<lockable_resource> > lock_table;      
unordered_map<int, std::string> type_table({{lockType::EXCLUSIVE,"EXCLUSIVE"},
                                            {lockType::SHARED,"SHARED"}});
unordered_map<int, std::string> status_table({{lockStatus::GRANTED,"GRANTED"},
                                              {lockStatus::WAITING,"WAITING"}});

int main(){  
    system("clear");
    cout<<"(Transaction_ID, Lock_Type, Lock_Status)\n"<<endl;

    lock("AAA", 1234, EXCLUSIVE);
    printStatus("AAA");

    lock("AAA", 4567, EXCLUSIVE);
    printStatus("AAA");
    
    unlock("AAA",1234);
    printStatus("AAA");

    lock("AAA", 1234, EXCLUSIVE);
    printStatus("AAA");

    downgrade("AAA", 4567);
    printStatus("AAA");

    lock("AAA",9876, SHARED);
    printStatus("AAA");
    
    upgrade("AAA", 9876);
    printStatus("AAA");
    
    cout<<endl;
    return 0;
}

// Function to print the status of each transcation in the list that is mapped with the resource_name.
void printStatus(std::string resource_name){
    if (lock_table.find(resource_name) != lock_table.end()){
        list<lockable_resource> lst = lock_table[resource_name];
        std::list<lockable_resource>::iterator iter;
        
        for(iter = lst.begin(); iter!=lst.end(); ++iter){
            cout<<"("<<iter->getTxnId()<<","<<type_table[iter->getLockType()]<<","<<status_table[iter->getStatus()]<<")\t";
        }
        cout<<endl;
    }
    cout<<endl;
}

void lock(std::string resource_name, int txn_id, lockType lock_type){
    lockStatus retval = WAITING;
    // Check if lock exists. 
    // No: Add to map, create new list and add lockable_resource to list
    
    cout<<"\033[102m"<<"Lock("<<resource_name<<","<<txn_id<<","<<type_table[lock_type]<<")"<<"\033[0m"<<endl;

    if (lock_table.find(resource_name) == lock_table.end()){
        // if the resource does not exist in the lock table => 
        //     locable resource has to be created. 
        //     list of lockable resources has to be created. 
        //     lock table should be updated with resource. 
        lockable_resource lr(txn_id, lock_type,GRANTED);
        retval = GRANTED;
        list<lockable_resource> lst;
        lst.emplace_back(lr);
        lock_table[resource_name] = lst;
    }

    else{
        // Resource is existing in the lock table.
        list<lockable_resource> lst = lock_table[resource_name];

        // check lock compatibility matrix
        std::list<lockable_resource>::iterator iter;

        for(iter = lst.begin(); iter!=lst.end(); ++iter){
            lockType lct = iter->getLockType();
            lockStatus lcs = iter->getStatus();

            if (lct==0 && lock_type==0){
                // If the lock type is Shared and lock status is Granted.
                // Grant a Shared lock to the incoming transaction.
                // Push the transaction to the front of the list.
                retval = GRANTED;
                lockable_resource lcr(txn_id, lock_type, retval);
                lst.emplace_front(lcr);
                lock_table[resource_name] = lst;
                return;     // return to avoid infinite loop
            } 
            else{
                // For all the other cases:
                // Make the transaction waiting by pushing it to the back of the list
                lockable_resource lcr(txn_id, lock_type, retval);
                lst.emplace_back(lcr);
                lock_table[resource_name] = lst;  
                return;     // return to avoid infinite loop
            }
        }
    }
    return;
}

void unlock(std::string resource_name, int txn_id){
    cout<<"\033[101m"<<"Unlock("<<resource_name<<","<<txn_id<<")"<<"\033[0m"<<endl;

    // Check if the resource is present in the lock table or not.
    if (lock_table.find(resource_name) != lock_table.end()){
        
        list<lockable_resource> lst = lock_table[resource_name];
        std::list<lockable_resource>::iterator iter;
        
        // Delete the transaction by iterating through the list
        for(iter=lst.begin();iter!=lst.end();++iter){
            if(txn_id==iter->getTxnId()){
                lst.erase(iter);
                break;
            }
        }

        iter = lst.begin();
        // If the first transaction is Waiting and in an Exclusive state:
        // Then Grant the lock and return
        if(iter->getStatus()==1 && iter->getLockType()==1){
                iter->setLockStatus(GRANTED);
                lock_table[resource_name] = lst;
                return;
        }

        // If the first transaction is Waiting and in a Shared state:
        // Grant the lock to all the transactions that are Waiting for a lock with Shared state.
        // After granting the lock, shift the transaction to the front of the list.
        else if(iter->getStatus()==1 && iter->getLockType()==0){
            for(iter=lst.begin();iter!=lst.end();){
                lockStatus lcs = iter->getStatus();
                lockType lct = iter->getLockType();
                if(lcs==1 && lct==0){
                    iter->setLockStatus(GRANTED);
                    auto temp = *iter;
                    iter = lst.erase(iter);
                    lst.emplace_front(temp); // iter points to the next transaction 
                    continue;       
                }
                ++iter;
            }
        }
        lock_table[resource_name] = lst;
    }
    else
        cout<<"\nThe resource is not present in the lock table to perform Unlock.\n";  
}

// function to upgrade the lock type of the resource to the transaction
void upgrade(std::string resource_name, int txn_id){
    cout<<"\033[104m"<<"Upgrade("<<resource_name<<","<<txn_id<<")"<<"\033[0m"<<endl;

    // Check if the resource is present in the lock table or not.
    if (lock_table.find(resource_name) != lock_table.end()){
        
        list<lockable_resource> lst = lock_table[resource_name];
        std::list<lockable_resource>::iterator iter;

        iter = lst.begin();
        // if the first transaction in the list was Granted a Shared lock, 
        // and the next transaction is Waiting for an Exclusive lock
        if(txn_id == iter->getTxnId() && iter->getStatus()==GRANTED && iter->getLockType()==SHARED){
            ++iter;
            // checking if the next transaction is Waiting for an Exclusive lock
            if(iter->getLockType()== EXCLUSIVE){
                --iter;
                iter->setLockType(EXCLUSIVE);
            }                 
        }
        lock_table[resource_name] = lst;
    }
    else
        cout<<"\nThe resource is not present in the lock table to perform Unlock.\n"; 
}

// function to downgrade the lock type of the resource to the transaction
void downgrade(std::string resource_name, int txn_id){
    cout<<"\033[103m"<<"Downgrade("<<resource_name<<","<<txn_id<<")"<<"\033[0m"<<endl;

    // Check if the resource is present in the lock table or not.
    if (lock_table.find(resource_name) != lock_table.end()){
        
        list<lockable_resource> lst = lock_table[resource_name];
        std::list<lockable_resource>::iterator iter;

        iter = lst.begin();
        if(txn_id == iter->getTxnId() && iter->getStatus()==GRANTED && iter->getLockType()==EXCLUSIVE){
            iter->setLockType(SHARED);

            //Grant lock to all the transactions that are Waiting for the Shared lock
            for(iter=++iter; iter!=lst.end(); ++iter){
                if(iter->getLockType()==SHARED && iter->getStatus()==WAITING)
                    iter->setLockStatus(GRANTED);
            }               
        }
        lock_table[resource_name] = lst;
    }
    else
        cout<<"\nThe resource is not present in the lock table to perform downgrade.\n"; 
}