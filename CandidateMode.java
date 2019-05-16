package edu.duke.raft;

import java.util.*;


public class CandidateMode extends RaftMode {
  
  private Timer myTimer;
  private String me = "CANDIDATE";
  
  public void go () {
    synchronized (mLock) {

      mConfig.setCurrentTerm(mConfig.getCurrentTerm()+1,mID);
      RaftResponses.setTerm(mConfig.getCurrentTerm());
      
      //Clear old data
      //RaftResponses.clearAppendResponses(term);
      boolean hey = RaftResponses.clearVotes(mConfig.getCurrentTerm());
 
      System.out.println ("S" + 
        mID + 
        "." + 
        mConfig.getCurrentTerm() + 
        ": switched to candidate mode.");
      
      // request votes from other servers - do we have to call this for every single serverID?
      for(int requestedID = 1; requestedID <= mConfig.getNumServers(); requestedID++)
      {
        if(requestedID != mID)
        {
          //System.out.println("CANDIDATE LOOK HERE HEY WHAT");
          //System.out.println(RaftResponses.getRounds(term)[requestedID]);
          //RaftResponses.setRound(requestedID,1,term); // set round for each server
          remoteRequestVote(requestedID,
              mConfig.getCurrentTerm(), // candidateTerm
              mID, // candidateID
              mLog.getLastIndex(), // lastLogIndex
              mLog.getLastTerm()); // lastLogTerm
        }
      }
      Random rand = new Random(); 
      int random_time = rand.nextInt(ELECTION_TIMEOUT_MAX)+ELECTION_TIMEOUT_MIN;
      //int random_time = mConfig.getTimeoutOverride();      
      myTimer = scheduleTimer(random_time, mID);
    }   
  }

  // @param candidate’s term
  // @param candidate requesting vote
  // @param index of candidate’s last log entry
  // @param term of candidate’s last log entry
  // @return 0, if server votes for candidate; otherwise, server's
  // current term 
  public int requestVote (int candidateTerm, //candidate would return no
        int candidateID,
        int lastLogIndex,
        int lastLogTerm)
  {
    synchronized (mLock) //if it gets a requestVote from someone more up to date, drop out
    {
      int myTerm = mConfig.getCurrentTerm ();
      int myLastIndex = mLog.getLastIndex();
      int myLogTerm = mLog.getLastTerm();

      if(myTerm < candidateTerm)//advance term and step down  
      {
          mConfig.setCurrentTerm(candidateTerm, 0);
          myTimer.cancel();
          RaftMode stepdown = new FollowerMode();//step down
          RaftServerImpl.setMode(stepdown);
          return myTerm;
      }

      if(candidateTerm < myTerm || // if candidate is behind us in term 
        (lastLogTerm < myLogTerm) || //our log has a higher lastTerm
        (lastLogTerm == myLogTerm && lastLogIndex < myLastIndex)//we have has same last term and longer log
        )
      {
        return myTerm;
      }
      else//requesting server is more up to date or equal,step down 
      {
        mConfig.setCurrentTerm(candidateTerm, candidateID);
        myTimer.cancel();
        RaftMode stepdown = new FollowerMode();//step down
        RaftServerImpl.setMode(stepdown);
        return myTerm;
      }
    }
  }
  

  // @param leader’s term
  // @param current leader
  // @param index of log entry before entries to append
  // @param term of log entry before entries to append
  // @param entries to append (in order of 0 to append.length-1)
  // @param index of highest committed entry
  // @return 0, if server appended entries; otherwise, server's
  // current term
  public int appendEntries (int leaderTerm,
          int leaderID,
          int prevLogIndex,
          int prevLogTerm,
          Entry[] entries,
          int leaderCommit) {
    synchronized (mLock) 
    {
      
      int myTerm = mConfig.getCurrentTerm();
        int myPrevLogIndex = mLog.getLastIndex();
        int myPrevLogTerm = mLog.getLastTerm();
        //int senderRound = -1;
 
        if (leaderTerm < myTerm || 
           (leaderTerm == myTerm && (prevLogTerm < myPrevLogTerm)) ||
           (leaderTerm == myTerm && (prevLogTerm == myTerm) && (prevLogIndex < myPrevLogIndex)))//tell Leader we are more up to date and dont accept their append response 
        {
            return myTerm;
        }
        else //leader is more up to date, step down and append
        { 
              myTimer.cancel();
              //RaftResponses.setAppendResponse(mID,0,leaderTerm,senderRound);//why is this commented out?
              RaftMode stepdown = new FollowerMode();
              RaftServerImpl.setMode(stepdown);
              if(leaderCommit < myPrevLogIndex)//we have uncommited entires 
              {
                return mConfig.getCurrentTerm();
              }
              else
              {
                return 0;
              }
        } 
    }  
  }

  // @param id of the timer that timed out
  public void handleTimeout (int timerID) {
    synchronized (mLock) {
      if(timerID == mID)
      {
          myTimer.cancel();
          //System.out.println(me + " " + mID + " timed out!");
          int term = mConfig.getCurrentTerm();
      // keep checking if we got a majority yet
          int[] mVotes = RaftResponses.getVotes(term);
          //System.out.println("I'm " + mID + " and my votes are: " + Arrays.toString(mVotes));
          //if (mVotes) { // if the term in RaftResponses doesn't match what we think
          //    RaftMode stepdown = new FollowerMode();
          //    RaftServerImpl.setMode(stepdown);
          //    return;
          //}
          // calculate majority
          int numYeses = 0;//number of 0 votes in mVotes
          for(int ind = 1; ind <= mConfig.getNumServers(); ind++)
          {
            if(mVotes[ind] == 0 && ind != mID)//we got a vote
            {
              numYeses++;
            }
            if (mVotes[ind] > term) {
              RaftMode stepdown = new FollowerMode();
              RaftServerImpl.setMode(stepdown);
              return;
            }
          }
          numYeses++; // vote for myself
          if((double)numYeses/(mVotes.length-1) > .5)//we won election, become leader
          {
              RaftMode stepup = new LeaderMode();
              RaftServerImpl.setMode(stepup);
              return;
          } 
          else//we lost, increment term and start again
          {
              mConfig.setCurrentTerm(mConfig.getCurrentTerm()+1,mID);
              RaftResponses.setTerm(mConfig.getCurrentTerm());
              RaftResponses.clearVotes(mConfig.getCurrentTerm());
            
              // request votes from other servers
              for(int requestedID = 1; requestedID <= mConfig.getNumServers(); requestedID++)
              {
                if(requestedID != mID)
                {
                  remoteRequestVote(requestedID,
                  mConfig.getCurrentTerm(), // candidateTerm
                  mID, // candidateID
                  mLog.getLastIndex(), // lastLogIndex
                  mLog.getLastTerm()); // lastLogTerm
               }
             }            
            
              Random rand = new Random(); 
              int random_time = rand.nextInt(ELECTION_TIMEOUT_MAX)+ELECTION_TIMEOUT_MIN;
              //int random_time = mConfig.getTimeoutOverride();              
              myTimer = scheduleTimer(random_time, mID);
          }
      }
      else
      {
        throw new RuntimeException("Oops I didn't set this timer!");
      }
    }
  }
}
