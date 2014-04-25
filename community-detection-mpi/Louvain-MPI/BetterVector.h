
/* 
 * File:   BetterVector.h
 * Author: charith
 *
 * Created on April 24, 2014, 10:09 AM
 */

#include <vector>



#ifndef BETTERVECTOR_H
#define	BETTERVECTOR_H
using namespace std;

template <class T>class BetterVector {
public:

    BetterVector() {
        size = 0;
    }
    //BetterVector(const BetterVector& orig){};

    virtual ~BetterVector() {
    };
    std::vector<T> sizes;
    std::vector< std::vector<T> > vectors;
    int size;

    void extend(vector<T> &vector) {
        int s = vector.size();
        sizes.push_back(s);
        size += s;
        vectors.push_back(vector);
//        cout << "Added"<<endl;
//        
//        for(int i=0;i < vector.size();i++) {
//            cout << vectors[vectors.size() -1][i];
//        }
//        
//        cout<<endl;
       
    }

    T get(int idx) {
        if (idx >= size) {
            throw new exception();
        }

        int vi=0;
        int offset=0;

        int currentTotal=0;

        for (int i = 0; i < sizes.size(); i++) {
            currentTotal += sizes[i];
            if (currentTotal > idx) {
                vi = i;
                offset = idx - (currentTotal - sizes[i]);
               // cout<<i<<": " <<offset <<endl;
                return vectors[i][offset];
            }
        }



        return NULL;
    }
    
    vector<T>::iterator getPointer(int idx) {
        
        if (idx >= size) {
            throw new exception();
        }

        int vi=0;
        int offset=0;

        int currentTotal=0;

        for (int i = 0; i < sizes.size(); i++) {
            currentTotal += sizes[i];
            if (currentTotal > idx) {
                vi = i;
                offset = idx - (currentTotal - sizes[i]);
               // cout<<i<<": " <<offset <<endl;
                return vectors[i].begin() + offset;
            }
        }
        
        return NULL;
    }
    
private:

};

#endif	/* BETTERVECTOR_H */

