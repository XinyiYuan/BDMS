/**
 * @file 3_StuID_hw2.cc
 * @author  XinyiYuan
 * @version 0.1
 * 
 * @section DESCRIPTION
 * 
 * This file implements the DirectedTriangleCount algorithm using graphlite API.
 *
 */
#include <iostream>
#include <cstdio>
#include <cstring>
#include <cmath>
#include <vector>
#include <set>

#include "GraphLite.h"

#define VERTEX_CLASS_NAME(name) DirectedTriangleCount##name

// num_in = num_out = num_through
typedef struct MyNode{
    int num_in_out_thr;
    int num_cycle;
}MyNode;

class VERTEX_CLASS_NAME(InputFormatter): public InputFormatter {
public:
    int64_t getVertexNum() {
        unsigned long long n;
        sscanf(m_ptotal_vertex_line, "%lld", &n);
        m_total_vertex= n;
        return m_total_vertex;
    }
    int64_t getEdgeNum() {
        unsigned long long n;
        sscanf(m_ptotal_edge_line, "%lld", &n);
        m_total_edge= n;
        return m_total_edge;
    }
    int getVertexValueSize() {
        m_n_value_size = sizeof(MyNode);
        return m_n_value_size;
    }
    int getEdgeValueSize() {
        m_e_value_size = sizeof(double);
        return m_e_value_size;
    }
    int getMessageValueSize() {
        m_m_value_size = sizeof(int64_t);
        return m_m_value_size;
    }
    void loadGraph() {
        unsigned long long last_vertex;
        unsigned long long from;
        unsigned long long to;
        double weight = 0;
        
        MyNode value;
        int outdegree = 0;
        
        const char *line= getEdgeLine();

        // Note: modify this if an edge weight is to be read
        //       modify the 'weight' variable

        sscanf(line, "%lld %lld", &from, &to);
        addEdge(from, to, &weight);

        last_vertex = from;
        ++outdegree;
        for (int64_t i = 1; i < m_total_edge; ++i) {
            line = getEdgeLine();

            // Note: modify this if an edge weight is to be read
            //       modify the 'weight' variable

            sscanf(line, "%lld %lld", &from, &to);
            if (last_vertex != from) {
                addVertex(last_vertex, &value, outdegree);
                last_vertex = from;
                outdegree = 1;
            } else {
                ++outdegree;
            }
            addEdge(from, to, &weight);
        }
        addVertex(last_vertex, &value, outdegree);
    }
};

class VERTEX_CLASS_NAME(OutputFormatter): public OutputFormatter {
public:
    void writeResult() {
        int64_t vid;
        MyNode value;
        char s[1024];

        ResultIterator r_iter;
        r_iter.getIdValue(vid, &value);
        int n = sprintf(s, "in: %d\nout: %d\nthrough: %d\ncycle: %d\n", value.num_in_out_thr, value.num_in_out_thr, value.num_in_out_thr, value.num_cycle);
        writeNextResLine(s, n);
    }
};

// An aggregator that records a double value tom compute sum
class VERTEX_CLASS_NAME(Aggregator): public Aggregator<MyNode> {
public:
    /** Initialize, mainly for aggregator value. */
    void init() {
        memset(&m_global, 0, sizeof(MyNode));
        memset(&m_local, 0, sizeof(MyNode));
    }
    
    /**
    * Get aggregator global value.
    * @return pointer of aggregator global value
    */
    void* getGlobal() {
        return &m_global;
    }
    
    /**
    * Set aggregator global value.
    * @param p pointer of value to set global as
    */
    void setGlobal(const void* p) {
        memmove(&m_global, p, sizeof(MyNode));
    }
    
    /**
    * Get aggregator local value.
    * @return pointer of aggregator local value
    */
    void* getLocal() {
        return &m_local;
    }
    
    /**
    * Merge method for global.
    * @param p pointer of value to be merged
    */
    void merge(const void* p) {
        m_global.num_in_out_thr += ((MyNode *)p)->num_in_out_thr;
        m_global.num_cycle += ((MyNode *)p)->num_cycle;
    }
    
    /**
    * Accumulate method for local.
    * @param p pointer of value to be accumulated
    */
    void accumulate(const void* p) {
        m_local.num_in_out_thr += ((MyNode *)p)->num_in_out_thr;
        m_local.num_cycle += ((MyNode *)p)->num_cycle;
    }
};

class VERTEX_CLASS_NAME(): public Vertex <MyNode, double, int64_t> {
public:
    void compute(MessageIterator* pmsgs) {
        int64_t vertexID = getVertexId();
        
        if (getSuperstep() == 0) {
           sendMessageToAllNeighbors(m_pme->m_v_id);
        }
        
        else if (getSuperstep() == 1){
            for ( ; !pmsgs->done(); pmsgs->next() ) {
                sendMessageToAllNeighbors(pmsgs->getValue());
                sendMessageTo(m_pme->m_v_id, pmsgs->getValue());
            }
        }
        
        else if (getSuperstep() == 2){
            vector<int64_t> prev_prev;
            set<int64_t> prev;
            set<int64_t> succ;
            
            for(; !pmsgs->done(); pmsgs->next()){
                if(((Msg *)pmsgs->getCurrent())->s_id == m_pme->m_v_id)
                    prev.insert(pmsgs->getValue());
                else
                    prev_prev.push_back(pmsgs->getValue());
            }
            
            for(auto succ_iter=getOutEdgeIterator(); !succ_iter.done(); succ_iter.next())
                succ.insert(succ_iter.target());
            
            MyNode acc={0,0};
            for(auto prev_prev_iter=prev_prev.begin(); prev_prev_iter!=prev_prev.end(); ++prev_prev_iter){
                if(prev.find(*prev_prev_iter) != prev.end())
                    acc.num_in_out_thr++;
                if (succ.find(*prev_prev_iter) != succ.end())
                    acc.num_cycle++;
            }
            accumulateAggr(0, &acc);
        }
        
        else if (getSuperstep() == 3){
            * mutableValue() = * (MyNode *)getAggrGlobal(0);
            voteToHalt();
        }
    }
};

class VERTEX_CLASS_NAME(Graph): public Graph {
public:
    VERTEX_CLASS_NAME(Aggregator)* aggregator;

public:
    // argv[0]: PageRankVertex.so
    // argv[1]: <input path>
    // argv[2]: <output path>
    void init(int argc, char* argv[]) {

        setNumHosts(5);
        setHost(0, "localhost", 1411);
        setHost(1, "localhost", 1421);
        setHost(2, "localhost", 1431);
        setHost(3, "localhost", 1441);
        setHost(4, "localhost", 1451);

        if (argc < 3) {
           printf ("Usage: %s <input path> <output path>\n", argv[0]);
           exit(1);
        }

        m_pin_path = argv[1];
        m_pout_path = argv[2];

        aggregator = new VERTEX_CLASS_NAME(Aggregator)[1];
        regNumAggr(1);
        regAggr(0, &aggregator[0]);
    }

    void term() {
        delete[] aggregator;
    }
};

/* STOP: do not change the code below. */
extern "C" Graph* create_graph() {
    Graph* pgraph = new VERTEX_CLASS_NAME(Graph);

    pgraph->m_pin_formatter = new VERTEX_CLASS_NAME(InputFormatter);
    pgraph->m_pout_formatter = new VERTEX_CLASS_NAME(OutputFormatter);
    pgraph->m_pver_base = new VERTEX_CLASS_NAME();

    return pgraph;
}

extern "C" void destroy_graph(Graph* pobject) {
    delete ( VERTEX_CLASS_NAME()* )(pobject->m_pver_base);
    delete ( VERTEX_CLASS_NAME(OutputFormatter)* )(pobject->m_pout_formatter);
    delete ( VERTEX_CLASS_NAME(InputFormatter)* )(pobject->m_pin_formatter);
    delete ( VERTEX_CLASS_NAME(Graph)* )pobject;
}
