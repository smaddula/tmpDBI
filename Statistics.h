#ifndef STATISTICS_
#define STATISTICS_
#include "ParseTree.h"
#include <map>
#include <set>
#include <string>
#include <utility>
#include <vector>
#include <iostream>
using namespace std;
class Relation
{
	unsigned long numtuples;
	map<string, unsigned long> attdistinctCount;
	string originalName;
 
	public:

		Relation ()  {
			numtuples = 0;
		}

  		explicit Relation (unsigned long tuples, string origNme)
  		{
			numtuples = tuples;
			originalName = origNme;
		}

  		map<string, unsigned long> const GetAtts()
    		{
      			return attdistinctCount;
    		}
  		
		void CopyAtts (Relation const & otherRel)
		{
    			attdistinctCount.insert(otherRel.attdistinctCount.begin(), otherRel.attdistinctCount.end());
  		}
  		
		unsigned long GetDistinct (string attr)
  		{
    			return attdistinctCount[attr];
  		}
  
		void AddAtt (string attr, unsigned long numDistinct)
  		{
    			attdistinctCount[attr] = numDistinct;
  		}
  
		unsigned long NumTuples() 
		{
			return numtuples;
		}
  		

  		friend ostream& operator<<(ostream& os, const Relation & RI)
    		{
      			map<string, unsigned long>::const_iterator it;
      			//os << RI.attdistinctCount.size() << endl;
      			for (it = RI.attdistinctCount.begin(); it != RI.attdistinctCount.end(); it++ )
        		{
          			//os << (*it).first << endl << (*it).second << endl;
        		}
      			return os;
    		}

  		friend istream& operator>>(istream& is, Relation & RI)
    		{
      			unsigned long numTups;
      			is >> numTups;
      			RI.numtuples = numTups;
      			unsigned long numMappings;
      			is >> numMappings;
      			for (unsigned i = 0; i < numMappings; i++)
        		{
          			string attr;
          			unsigned long distinct;
          			is >> attr;
          			is >> distinct;
          			RI.attdistinctCount[attr] = distinct;
        		}
      			return is;
    		}
};

class Statistics
{
	private:

  		map < string, Relation > rels;
  		map < string, string> relationAttrsMap; 
  		map < string, string> mergedRelations;
  		void Check (struct AndList *parseTree, char *relNames[], int numToJoin);
  		void CheckRelations(char *relNames[], int numToJoin);
  		vector<string> CheckParseTree(struct AndList *pAnd);
  		double CalculateEstimate(struct AndList *pAnd);
  		bool HasJoin(AndList *pAnd);
 	public:
  		Statistics();
  		Statistics(Statistics &copyMe); // Performs deep copy
  		~Statistics();


  		void AddRel(char *relName, int numTuples);
  		void AddAtt(char *relName, char *attName, int numDistincts);
  		void CopyRel(char *oldName, char *newName);

  		void Read(char *fromWhere);
  		void Write(char *fromWhere);

  		void  Apply(struct AndList *parseTree, char *relNames[], int numToJoin);
  		double Estimate(struct AndList *parseTree, char **relNames, int numToJoin);

  		string getAttrHomeTable(string a)
    		{
      			if (1 == relationAttrsMap.count(a))
      	  		{
          			string tbl(relationAttrsMap[a]);
          			return tbl;
        		}
      			return "";
    		}
};

#endif
