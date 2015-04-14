#include "Statistics.h"
#include <cassert>
#include <string>
#include <iostream>
#include <cstdlib>
#include <vector>
#include <set>
#include <tr1/unordered_set>
#include <algorithm>
#include <fstream>

using namespace std;
Statistics::Statistics() 
 {
}

Statistics::Statistics(Statistics &copyMe ) 
 {
}
Statistics::~Statistics()
{
}

void Statistics::AddRel(char *relName, int numTuples)
{
  string s(relName);
  const Relation newRelation (numTuples, s);
  rels[s] = newRelation;
  rels.insert( make_pair(s,newRelation));

}
void Statistics::AddAtt(char *relName, char *attName, int numDistincts)
{
  string const rel(relName);
  string const att(attName);
  if (-1 == numDistincts)
    {
      // relationInformation[rel] = make_pair(att, relations[rel]);
      rels[rel].AddAtt(att,rels[rel].NumTuples());
      extantAttrs.insert(make_pair(att,rel));
    }
  else
    {
      rels[rel].AddAtt(att,numDistincts);
      extantAttrs.insert(make_pair(att,rel));
    }
}
void Statistics::CopyRel(char *oldName, char *newName)
{
  string oldN(oldName);
  string newN(newName);
  map < std::string, unsigned long > const oldAttrs = rels[oldN].GetAtts();

  Relation newR(rels[oldN].NumTuples(), oldN);

  map < std::string, unsigned long >::const_iterator it;
  for (it = oldAttrs.begin(); it != oldAttrs.end(); it++ )
    {
      string newAttrName(newN+"."+(*it).first); // might need to do this for the schma variables as well.
      //clog << "aliasing " << newAttrName << newAttrName.size() << " to " << newN << endl;
      newR.AddAtt(newAttrName, (*it).second); // add modified attr to new relation
      extantAttrs[newAttrName] = newN; // know where these modified attrs are
    }

  rels[newN] = newR; // put new relation in
}

void Statistics::Read(char *fromWhere)
{
  //clog << endl;
  ifstream statFile(fromWhere);

  if (!statFile.good())
    {
      return;
    }

  unsigned iters;
  statFile >> iters;
  for(unsigned i = 0; i < iters; i++)
    {
      string relation;
      Relation RI;
      statFile >> relation;
      statFile >> RI;
      rels[relation] = RI;
    }
  statFile >> iters;

  for(unsigned i = 0; i < iters; i++)
    {
      string attr;
      string relation;
      statFile >> attr >> relation;
      extantAttrs[attr] = relation;
    }
  statFile >> iters;
  for(unsigned i = 0; i < iters; i++)
    {
      string relation;
      string mergedrelation;
      statFile >> relation >> mergedrelation;
      mergedRelations[relation] = mergedrelation;
    }
}
void Statistics::Write(char *fromWhere)
{
  ofstream statFile(fromWhere);

  statFile << rels.size() << endl;
  {
    map < std::string, Relation >::iterator it;
    for (it = rels.begin(); it != rels.end(); it++ )
      {
        statFile << (*it).first << endl << (*it).second << endl;
      }
  }
  statFile << extantAttrs.size() << endl;
  {
    map < std::string, std::string>::iterator it;
    for (it = extantAttrs.begin(); it != extantAttrs.end(); it++ )
      {
        statFile << (*it).first << endl << (*it).second << endl;
      }
  }
  statFile << mergedRelations.size() << endl;
  {
    map < std::string, std::string>::const_iterator it;
    for (it = mergedRelations.begin(); it != mergedRelations.end(); it++ )
      {
        statFile << (*it).first << endl << (*it).second << endl;
      }
  }

  statFile.close();
}

void  Statistics::Apply(struct AndList *parseTree, char *relNames[], int numToJoin)
{
  CheckRelations(relNames, numToJoin);
  vector <std::string> attrs = CheckParseTree(parseTree);
  double estimate = 0;
  if (0 == parseTree and 2 >= numToJoin)
    {
      double accumulator = 1.0l;
      for (signed i = 0; i < numToJoin; i++)
        {
          string rel(relNames[i]);
          accumulator *= rels[rel].NumTuples();
        }
      estimate = accumulator;
    }
  else
    {
      estimate = CalculateEstimate(parseTree);
    }
  // make new name for joined relation.
  string newRelation;
  if(HasJoin(parseTree))
    {
      for (signed i = 0; i < numToJoin; i++)
        {
          string rel(relNames[i]);
          newRelation += rel;
        }
      //clog << "new relation is " << newRelation << endl;
      // new map, to have both relations merged into it.
      Relation merged(estimate,newRelation); // new relation with estimated

      for (int i = 0; i < numToJoin ; i++)
        {
          merged.CopyAtts(rels[relNames[i]]);
        }
      rels[newRelation] = merged;

      { // get rid of information about old relations
      vector<std::string> attrsInParseTree  = CheckParseTree(parseTree);
      // create a set of old relations to steal attributes from.
      set<string> oldRels;
      for(vector<string>::iterator it = attrsInParseTree.begin(); it < attrsInParseTree.end(); ++it)
        {
          oldRels.insert(extantAttrs[(*it)]);
        }

      for(set<string>::const_iterator it = oldRels.begin(); it != oldRels.end(); ++it)
        {
          merged.CopyAtts(rels[*it]);
          rels.erase((*it));
        }
      for (int i = 0; i < numToJoin ; i++)
        {
          rels.erase(relNames[i]); //
          mergedRelations[relNames[i]] = newRelation;
        }
      }

      map<std::string, unsigned long> mergedAtts = merged.GetAtts();

      map < std::string, unsigned long>::const_iterator it;
      for (it = mergedAtts.begin(); it != mergedAtts.end(); it++ )
        {
          extantAttrs[(*it).first] = newRelation;
          //clog << (*it).first << "now belongs to " << newRelation << endl;
        }
    }


  }
double Statistics::Estimate(struct AndList *parseTree, char **relNames, int numToJoin)
{
  if (0 == parseTree and 2 >= numToJoin)
    {
      double accumulator = 1.0l;
      for (signed i = 0; i < numToJoin; i++)
        {
          string rel(relNames[i]);
          accumulator *= rels[rel].NumTuples();
        }
      return accumulator;
    }

  CheckRelations(relNames, numToJoin);
  vector <std::string> attrs = CheckParseTree(parseTree);
  
   double result = CalculateEstimate(parseTree);
  return result;
}

void Statistics :: Check (struct AndList *parseTree, char *relNames[], int numToJoin)
{
  CheckRelations(relNames, numToJoin);
  CheckParseTree(parseTree);
  return;
}

void Statistics :: CheckRelations(char *relNames[], int numToJoin)
{

  for (int i = 0; i < numToJoin; i++)
    {
      string rel(relNames[i]);
      if (0 == rels.count(rel) and 0 == mergedRelations.count(rel))
        {
          exit(-1);
        }
    }
}

vector<std::string> Statistics :: CheckParseTree(struct AndList *pAnd)
{
  vector < std::string > attrs;
  attrs.reserve(100);
  while (pAnd)
    {
      struct OrList *pOr = pAnd->left;
      while (pOr)
        {
          struct ComparisonOp *pCom = pOr->left;
          if (pCom!=NULL)
            {
              {
                struct Operand *pOperand = pCom->left;
                if(pOperand!=NULL && (NAME == pOperand->code))
                  {
                    // check left operand
                    string attr(pOperand->value);
                    if (0 == extantAttrs.count(attr))
                      {
                        cerr << "left operand attribute \"" << attr << "\" not found" << std::endl;
                        assert(0 != extantAttrs.count(attr));
                        exit(-1);
                      }
                    attrs.push_back(attr);
                  }
              }
              {
                struct Operand *pOperand = pCom->right;
                if(pOperand!=NULL && (NAME == pOperand->code))
                  {
                    // check right operand
                    string attr(pOperand->value);
                    if (0 == extantAttrs.count(attr))
                      {
                        cerr << "right operand attribute \"" << attr << "\" not found" << std::endl;
                        assert(0 != extantAttrs.count(attr));
                        exit(-1);
                      }
                    attrs.push_back(attr);
                  }
              }
            }
          pOr = pOr->rightOr;
        }
      pAnd = pAnd->rightAnd;
    }
  return attrs; // return by copy
}

double Statistics :: CalculateEstimate(AndList *pAnd)
{
  double result = 1.0l;
  bool seenJoin = false;
  double selectOnlySize = 0.0l;
  while (pAnd)
    {
      OrList *pOr = pAnd->left;
      bool independentORs = true; // assume independence
      bool singleOR = false;
      //clog << "singleOr is " << singleOR << endl;
      { // but check
        set <string> ors;
        unsigned count = 0;
        while (pOr) // traverse with counter.
          {
            ComparisonOp *pCom = pOr->left;
            if (pCom!=NULL)
              {
                //clog << count;
                count++;
                string attr(pOr->left->left->value);
                //clog << "orattr is " << attr << endl;
                //clog << "or.size is " << ors.size() << endl;
                ors.insert(attr);
              }
            pOr = pOr->rightOr;
          }
        if (ors.size() != count)
          {independentORs = false;}
        if (1 == count)
          {independentORs = false; 
	//clog << "singleOr is " << singleOR << endl; 
	singleOR = true; 
	//clog << "singleOr is " << singleOR << endl; 
	//clog << "THERE IS A SINGLE OR" << endl; 
	//clog << "singleOr is " << singleOR << endl;
	}
        //clog << " ors are ";
        /*if(independentORs)
          clog << "independent" << endl;
        else
          clog << "dependent" << endl;*/
      }
      //clog << "singleOr is " << singleOR << endl;
      pOr = pAnd->left; // reset pointer
      double tempOrValue = 0.0l; // each or is calculated separately, and then multiplied in at the end.
      if(independentORs)
        {tempOrValue = 1.0l;}
      while (pOr)
        {
          struct ComparisonOp *pCom = pOr->left;
          if (pCom!=NULL)
            {
              // pcom has left and right, as well as the operand that
              // details what it is an equality signals either a join
              // or a selection, geq (>=) or leq (<=) are both
              // selections (or really dumb joins that I am not going
              // to cover)
              Operand *lOperand = pCom->left;
              Operand *rOperand = pCom->right;
              switch(pCom->code)
                {
                case EQUALS: // maybe selection or maybe join
                  {
                    if ((0 != lOperand and (NAME == lOperand->code)) and
                        (0 != rOperand and (NAME == rOperand->code)))
                      {// this is a join, because both the left and right are attribute names
                        //clog << endl << "join case estimation" << endl << endl;
                        seenJoin = true;
                        string const lattr(lOperand->value);
                        string const rattr(rOperand->value);
                        // look up which relation l attr is in
                        string const lrel = extantAttrs[lattr];
                        // get size of l relation
                        unsigned long const lRelSize = rels[lrel].NumTuples();
                        // get number of Distinct values of L attr
                        int const lDistinct = rels[lrel].GetDistinct(lattr);
                        // look up which relation r attr is in
                        string const rrel = extantAttrs[rattr];
                        // get size of r relation
                        unsigned long const rRelSize = rels[rrel].NumTuples();
                        // get number of Distinct values of R attr
                        int const rDistinct = rels[rrel].GetDistinct(rattr);

                        //clog << "lr = " << lRelSize << " rr = " << rRelSize << endl;
                        //clog << "product is " << ((double)lRelSize * (double)rRelSize) << endl;

                        double numerator   = lRelSize * rRelSize;
                        double denominator = max(lDistinct,rDistinct);

                        //clog << "lattr of " << lattr << " with " << lDistinct <<" distinct values is "
                             //<< "found in rel " << lrel << " of size " << lRelSize << endl;
                        //clog << "rattr of " << rattr << " with " << rDistinct <<" distinct values is "
                             //<< "Found in rel " << rrel << " of size " << rRelSize << endl;
                        tempOrValue += (numerator/denominator);
                        //clog << "numerator is " << numerator
                             //<< " denominator is " << denominator
                             //<< " with final result of " << tempOrValue << endl << endl;
                      }
                    else
                      { // this is a selection // maybe fall through?
                        //clog << endl <<  "*** EQUALITY SELECTION" << endl;
                        Operand *opnd = 0;
                        Operand *constant = 0;
                        if (NAME == lOperand->code)
                          {opnd = lOperand; constant = rOperand; }
                        else if (NAME == rOperand->code)
                          {opnd = rOperand; constant = lOperand;}
                        assert(0 != opnd); // something was assigned
                        assert(0 != constant); // something was assigned

                        string const attr(opnd->value);
                        string const relation = extantAttrs[attr];
                        unsigned long const distinct = rels[relation].GetDistinct(attr);
                        //clog << "singleOr is " << singleOR << endl;
                        if (singleOR)
                          {
                            double const calculation = (1.0l/distinct);// (numerator/denominator);

                            //clog << "single value is " << calculation << endl;
                            tempOrValue += calculation;
                          }
                        else
                          {
                            if(independentORs) // independent ORs
                              {
                                double const calculation = (1.0l - (1.0l/distinct));
                                //clog << "indep, value is " << calculation << endl;
                                tempOrValue *= calculation;
                              }
                            else // dependent ORs
                              {
                                // else
                                {
                                  double const calculation = (1.0l/distinct);
                                  //clog << "dep, value is " << calculation << endl;
                                  tempOrValue += calculation;
                                }
                              }
                          }
                        //clog <<  "*** EQUALITY SELECTION end with result " << endl << endl;
                      }
                    break;
                  }
                case LESS_THAN: // selection
                  //break;
                case GREATER_THAN: // selection
                  // break;
                  //clog << "not equal selection fall through" << endl;
                  // we are in a selection now.
                  // so either of our operands could be a literal value rather than an attribute
                  Operand *opnd = 0;
                  Operand *constant = 0;
                  if (NAME == lOperand->code)
                    {opnd = lOperand; constant = rOperand; }
                  else if (NAME == rOperand->code)
                    {opnd = rOperand; constant = lOperand;}
                  assert(0 != opnd); // something was assigned
                  assert(0 != constant); // something was assigned

                  string const attr(opnd->value);
                  string const relation = extantAttrs[attr];

                  if(independentORs) // independent ORs
                    {
                      double const calculation = 1.0l - (1.0l)/(3.0l);;
                      //clog << "indep, value is " << calculation << endl;
                      tempOrValue *= calculation;
                    }
                  else // dependent ORs
                    {
                      double const calculation = (1.0l)/(3.0l);
                      //clog << "dep, value is " << calculation << endl;
                      tempOrValue += calculation;
                    }
                  break;
                }
              if (!seenJoin)
                {
                  Operand *opnd = 0;
                  if (NAME == lOperand->code)
                    {opnd = lOperand;}
                  else if (NAME == rOperand->code)
                    {opnd = rOperand;}
                  string const attr(opnd->value);
                  string const relation = extantAttrs[attr];
                  unsigned long const relationSize = rels[relation].NumTuples();
                  selectOnlySize = relationSize;
                }
              {
                struct Operand *pOperand = pCom->left;
                if(pOperand!=NULL and (NAME == pOperand->code))
                  {
                    // check left operand
                    string attr(pOperand->value);
                    if (0 == extantAttrs.count(attr))
                      {
                        cerr << "estimate left operand attribute \"" << attr << "\" not found" << std::endl;
                        assert(0 != extantAttrs.count(attr));
                        exit(-1);
                      }
                  }
              }
              // operator
              {
                struct Operand *pOperand = pCom->right;
                if(pOperand!=NULL and (NAME == pOperand->code))
                  {
                    // check right operand
                    string attr(pOperand->value);
                    if (0 == extantAttrs.count(attr))
                      {
                        cerr << "estimate right operand attribute \"" << attr << "\" not found" << std::endl;
                        assert(0 != extantAttrs.count(attr));
                        exit(-1);
                      }
                  }
              }
            }
          pOr = pOr->rightOr; // go to next or
        }
      //clog << "putting ors into and estimate" << endl;
      if (independentORs)
        {
          //clog << "independent ors" << endl;
          //clog << "before, result was " << result << endl;
          result *= (1 - tempOrValue);
          //clog << "after, result was " << result << endl;
        }
      else
        {
          //clog << "dependent ors" << endl;
          //clog << "before, result was " << result << endl;
          result *= tempOrValue;
          //clog << "after, result was " << result << endl;
        }
      pAnd = pAnd->rightAnd; // go to next and
    }
  if (!seenJoin)
    {
      result *= selectOnlySize;
    }
  return result;
}

bool Statistics :: HasJoin(AndList *pAnd)
{
  double result = 1.0l;
  bool seenJoin = false;
  double selectOnlySize = 0.0l;
  while (pAnd)
    {
      OrList *pOr = pAnd->left;
      bool independentORs = true; // assume independence
      bool singleOR = false;
      //clog << "singleOr is " << singleOR << endl;
      { // but check
        set <string> ors;
        unsigned count = 0;
        while (pOr) // traverse with counter.
          {
            ComparisonOp *pCom = pOr->left;
            if (pCom!=NULL)
              {
                //clog << count;
                count++;
                string attr(pOr->left->left->value);
                //clog << "orattr is " << attr << endl;
                //clog << "or.size is " << ors.size() << endl;
                ors.insert(attr);
              }
            pOr = pOr->rightOr;
          }
        if (ors.size() != count)
          {independentORs = false;}
        if (1 == count)
          {independentORs = false; 
	//clog << "singleOr is " << singleOR << endl; 
	singleOR = true; 
	//clog << "singleOr is " << singleOR << endl; 
	//clog << "THERE IS A SINGLE OR" << endl; 
	//clog << "singleOr is " << singleOR << endl;
	}
        //clog << " ors are ";
        /*if(independentORs)
          clog << "independent" << endl;
        else
          clog << "dependent" << endl;*/
      }
      //clog << "singleOr is " << singleOR << endl;
      pOr = pAnd->left; // reset pointer
      double tempOrValue = 0.0l; // each or is calculated separately, and then multiplied in at the end.
      if(independentORs)
        {tempOrValue = 1.0l;}
      while (pOr)
        {
          struct ComparisonOp *pCom = pOr->left;
          if (pCom!=NULL)
            {
              // pcom has left and right, as well as the operand that
              // details what it is an equality signals either a join
              // or a selection, geq (>=) or leq (<=) are both
              // selections (or really dumb joins that I am not going
              // to cover)
              Operand *lOperand = pCom->left;
              Operand *rOperand = pCom->right;
              switch(pCom->code)
                {
                case EQUALS: // maybe selection or maybe join
                  {
                    if ((0 != lOperand and (NAME == lOperand->code)) and
                        (0 != rOperand and (NAME == rOperand->code)))
                      {// this is a join, because both the left and right are attribute names
                        //clog << endl << "join case estimation" << endl << endl;
                        seenJoin = true;
                        string const lattr(lOperand->value);
                        string const rattr(rOperand->value);
                        // look up which relation l attr is in
                        string const lrel = extantAttrs[lattr];
                        // get size of l relation
                        unsigned long const lRelSize = rels[lrel].NumTuples();
                        // get number of Distinct values of L attr
                        int const lDistinct = rels[lrel].GetDistinct(lattr);
                        // look up which relation r attr is in
                        string const rrel = extantAttrs[rattr];
                        // get size of r relation
                        unsigned long const rRelSize = rels[rrel].NumTuples();
                        // get number of Distinct values of R attr
                        int const rDistinct = rels[rrel].GetDistinct(rattr);

                        //clog << "lr = " << lRelSize << " rr = " << rRelSize << endl;
                        //clog << "product is " << ((double)lRelSize * (double)rRelSize) << endl;

                        double numerator   = lRelSize * rRelSize;
                        double denominator = max(lDistinct,rDistinct);

                        //clog << "lattr of " << lattr << " with " << lDistinct <<" distinct values is "
                             //<< "found in rel " << lrel << " of size " << lRelSize << endl;
                        //clog << "rattr of " << rattr << " with " << rDistinct <<" distinct values is "
                             //<< "Found in rel " << rrel << " of size " << rRelSize << endl;
                        tempOrValue += (numerator/denominator);
                        //clog << "numerator is " << numerator
                             //<< " denominator is " << denominator
                             //<< " with final result of " << tempOrValue << endl << endl;
                      }
                    else
                      { // this is a selection // maybe fall through?
                        //clog << endl <<  "*** EQUALITY SELECTION" << endl;
                        Operand *opnd = 0;
                        Operand *constant = 0;
                        if (NAME == lOperand->code)
                          {opnd = lOperand; constant = rOperand; }
                        else if (NAME == rOperand->code)
                          {opnd = rOperand; constant = lOperand;}
                        assert(0 != opnd); // something was assigned
                        assert(0 != constant); // something was assigned

                        string const attr(opnd->value);
                        string const relation = extantAttrs[attr];
                        unsigned long const distinct = rels[relation].GetDistinct(attr);
                        //clog << "singleOr is " << singleOR << endl;
                        if (singleOR)
                          {
                            double const calculation = (1.0l/distinct);// (numerator/denominator);

                            //clog << "single value is " << calculation << endl;
                            tempOrValue += calculation;
                          }
                        else
                          {
                            if(independentORs) // independent ORs
                              {
                                double const calculation = (1.0l - (1.0l/distinct));
                                //clog << "indep, value is " << calculation << endl;
                                tempOrValue *= calculation;
                              }
                            else // dependent ORs
                              {
                                // else
                                {
                                  double const calculation = (1.0l/distinct);
                                  //clog << "dep, value is " << calculation << endl;
                                  tempOrValue += calculation;
                                }
                              }
                          }
                        //clog <<  "*** EQUALITY SELECTION end with result " << endl << endl;
                      }
                    break;
                  }
                case LESS_THAN: // selection
                  //break;
                case GREATER_THAN: // selection
                  // break;
                  //clog << "not equal selection fall through" << endl;
                  // we are in a selection now.
                  // so either of our operands could be a literal value rather than an attribute
                  Operand *opnd = 0;
                  Operand *constant = 0;
                  if (NAME == lOperand->code)
                    {opnd = lOperand; constant = rOperand; }
                  else if (NAME == rOperand->code)
                    {opnd = rOperand; constant = lOperand;}
                  assert(0 != opnd); // something was assigned
                  assert(0 != constant); // something was assigned

                  string const attr(opnd->value);
                  string const relation = extantAttrs[attr];

                  if(independentORs) // independent ORs
                    {
                      double const calculation = 1.0l - (1.0l)/(3.0l);;
                      //clog << "indep, value is " << calculation << endl;
                      tempOrValue *= calculation;
                    }
                  else // dependent ORs
                    {
                      double const calculation = (1.0l)/(3.0l);
                      //clog << "dep, value is " << calculation << endl;
                      tempOrValue += calculation;
                    }
                  break;
                }
              if (!seenJoin)
                {
                  Operand *opnd = 0;
                  if (NAME == lOperand->code)
                    {opnd = lOperand;}
                  else if (NAME == rOperand->code)
                    {opnd = rOperand;}
                  string const attr(opnd->value);
                  string const relation = extantAttrs[attr];
                  unsigned long const relationSize = rels[relation].NumTuples();
                  selectOnlySize = relationSize;
                }
              {
                struct Operand *pOperand = pCom->left;
                if(pOperand!=NULL and (NAME == pOperand->code))
                  {
                    // check left operand
                    string attr(pOperand->value);
                    if (0 == extantAttrs.count(attr))
                      {
                        cerr << "operand attribute \"" << attr << "\" not found" << std::endl;
                        assert(0 != extantAttrs.count(attr));
                        exit(-1);
                      }
                  }
              }
              // operator
              {
                struct Operand *pOperand = pCom->right;
                if(pOperand!=NULL and (NAME == pOperand->code))
                  {
                    // check right operand
                    string attr(pOperand->value);
                    if (0 == extantAttrs.count(attr))
                      {
                        cerr << "operand attribute \"" << attr << "\" not found" << std::endl;
                        assert(0 != extantAttrs.count(attr));
                        exit(-1);
                      }
                  }
              }
            }
          pOr = pOr->rightOr; // go to next or
        }
      //clog << "putting ors into and estimate" << endl;
      if (independentORs)
        {
          //clog << "independent ors" << endl;
          //clog << "before, result was " << result << endl;
          result *= (1 - tempOrValue);
          //clog << "after, result was " << result << endl;
        }
      else
        {
          //clog << "dependent ors" << endl;
          //clog << "before, result was " << result << endl;
          result *= tempOrValue;
          //clog << "after, result was " << result << endl;
        }
      pAnd = pAnd->rightAnd; // go to next and
    }
  if (!seenJoin)
    {
      result *= selectOnlySize;
    }
  return seenJoin;
}
