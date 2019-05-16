//how does indexing work?
//nextIndex array reinitialized as incremented
//looping to get subarray

package edu.duke.raft;
import java.util.*;

public class LeaderMode extends RaftMode {
  int num_servers = mConfig.getNumServers();
  private int[] nextIndex = new int[num_servers+1]; // store
  private Timer heartbeat;
  private String me = "LEADER";
  
  public void go () {
    synchronized (mLock) {
      // get current term
      int term = mConfig.getCurrentTerm();
      // send first heartbeat
      Entry[] entries = {}; // empty
      RaftResponses.clearAppendResponses(mConfig.getCurrentTerm());
      for (int s=1;s<=mConfig.getNumServers();s++) { // for every server
        if (mID != s) { // don't send heartbeat to myself
          //RaftResponses.setRound(s,1,term);
          this.remoteAppendEntries(s, term, mID, mLog.getLastIndex()+1, mLog.getLastTerm(), entries, mCommitIndex);
        }
      }

      // initialize nextIndex
      for (int i=1;i<=mConfig.getNumServers();i++) {
        nextIndex[i] = mLog.getLastIndex() + 1; // initialize to leader's last log index + 1
      }
      
      //System.out.println("mLog.getLastIndex() leader is " + mLog.getLastIndex());

      System.out.println ("S" + 
        mID + 
        "." + 
        term + 
        ": switched to leader mode.");

      // set up timer
      heartbeat = scheduleTimer((int) HEARTBEAT_INTERVAL, mID);
    }
  }
  
  // @param candidate’s term
  // @param candidate requesting vote
  // @param index of candidate’s last log entry
  // @param term of candidate’s last log entry
  // @return 0, if server votes for candidate; otherwise, server's
  // current term
  public int requestVote (int candidateTerm,
        int candidateID,
        int lastLogIndex,
        int lastLogTerm) {
    synchronized (mLock) {
      int myTerm = mConfig.getCurrentTerm();
      int myPrevLogIndex = mLog.getLastIndex();
      int myPrevLogTerm = mLog.getLastTerm();
        //int senderRound = -1;
      
      if(myTerm < candidateTerm)//advance term and step down  
      {
          mConfig.setCurrentTerm( candidateTerm , 0);
          heartbeat.cancel();
          RaftMode stepdown = new FollowerMode();//step down
          RaftServerImpl.setMode(stepdown);
          return myTerm;
      }

      if (myTerm > candidateTerm || 
         ((lastLogTerm < myPrevLogTerm)) ||
         ((lastLogTerm == myTerm) && (lastLogIndex < myPrevLogIndex)))
	       //tell candidate we are more up to date and dont accept their request vote 
        {              
            return myTerm;
        }
      else //candidate is more up to date, step down
      { 
            heartbeat.cancel();
            RaftMode stepdown = new FollowerMode();
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
    synchronized (mLock) {
      int myTerm = mConfig.getCurrentTerm();
      int myPrevLogIndex = mLog.getLastIndex();
      int myPrevLogTerm = mLog.getLastTerm();
      int senderRound = RaftResponses.getRounds(leaderTerm)[leaderID]; // indexes mRounds array
      int result;
      
      // if we receive an append entries
      // if leaderTerm < myTerm then reject
      // if leaderTerm == myTerm then ??? two leaders for the same term? check logs
        // if prevLogTerm > myPrevLogTerm then step down
        // if prevLogIndex > myPrevLogIndex then step down
        // else ?
      // if leaderTerm > myTerm then step down
      
      if (leaderTerm < myTerm) {
        result = myTerm;
        return result;
      }
      else if (leaderTerm == myTerm) { // if 2 leaders in 1 term, pick the leader with the more up to date log
        if (prevLogTerm > myPrevLogTerm) { // check latest term in log first
          result = myTerm;
          heartbeat.cancel();
          RaftMode stepdown = new FollowerMode();
          RaftServerImpl.setMode(stepdown);
          return result;
        }
        if (prevLogIndex > myPrevLogIndex) { // then check how long each log is
          result = myTerm;
          heartbeat.cancel();
          RaftMode stepdown = new FollowerMode();
          RaftServerImpl.setMode(stepdown);
          return result;
        }
        if (prevLogTerm == myPrevLogTerm && prevLogIndex == myPrevLogIndex) { // if somehow same term and identical logs
          if (leaderID > mID) { // the leader with the higher ID will step down
            heartbeat.cancel();
            result = myTerm;
            RaftMode stepdown = new FollowerMode();
            RaftServerImpl.setMode(stepdown);
            return result;
          }
          else {
            result = myTerm;
            return result;
          }
        }
      }
      else { // (leaderTerm > myTerm) -> step down and advance term 
          result = myTerm;
          heartbeat.cancel();
          mConfig.setCurrentTerm(leaderTerm, 0);
          RaftMode stepdown = new FollowerMode();
          RaftServerImpl.setMode(stepdown);
          return result;
      }
      return 0;
    }
  }

  // @param id of the timer that timed out
  public void handleTimeout (int timerID) {
    synchronized (mLock) {
      if (timerID == mID) {
        heartbeat.cancel();
        
        //System.out.println(me + " " + mID + " timed out!");
        // check last heartbeats' remoteAppendEntries
        int myTerm = mConfig.getCurrentTerm();
        int[] responses = RaftResponses.getAppendResponses(mConfig.getCurrentTerm());
       //System.out.println("Responses array is " + Arrays.toString(RaftResponses.getAppendResponses(mConfig.getCurrentTerm())));
        
        
        Entry[] entries = null;
        
        // figure out whose log needs to be repaired
        for (int i=1;i<=mConfig.getNumServers();i++) {
          entries = null;
          //System.out.println("Server "+i+"'s response is " + responses[i]);
          if (responses[i] != 0 && i != mID) { // if the server rejected my remoteAppendEntries and it's not me
            if (responses[i] > mConfig.getCurrentTerm()) { // if a follower is higher term than me
              mConfig.setCurrentTerm(responses[i], 0);
              RaftMode stepdown = new FollowerMode();
              RaftServerImpl.setMode(stepdown);
              return;
            }
            nextIndex[i]=nextIndex[i]-1;
            if (nextIndex[i]<0) nextIndex[i]=0; // make sure it doesn't go too low
            entries = new Entry[mLog.getLastIndex() - nextIndex[i] + 1]; // subarray from nextIndex+1 to end, inclusive
            for (int e=0 ; e<entries.length ; e++) {
              entries[e] = mLog.getEntry(e+nextIndex[i]);
            }
          }
          else if (responses[i] == 0 && i != mID)  // if the server accepted my remoteAppendEntries and it's not me*/
          {
            entries = null;
            nextIndex[i] = mLog.getLastIndex() + 1;
           //System.out.println("Leader "+ mID + " reset nextIndex for server "+i+" to be " +nextIndex[i]);
          }
          
          // now SEND
          //Entry[] entries = null;
          if (mID != i) { // if not me, send heartbeat
            //System.out.println(mID + " is sending heartbeat to server " + i + " with term " + myTerm + " and nextIndex = " + nextIndex[i] + " and entries " + Arrays.toString(entries));
            //remoteAppendEntries(i, mConfig.getCurrentTerm(), mID, mLog.getLastIndex(), mLog.getLastTerm(), entries, mCommitIndex);
            int prevTerm = -1;
            if (nextIndex[i]>0){
              prevTerm = mLog.getEntry(nextIndex[i]-1).term;
            }
            remoteAppendEntries(i, mConfig.getCurrentTerm(), mID, nextIndex[i], prevTerm, entries, mCommitIndex);
          }
        }
        RaftResponses.clearAppendResponses(mConfig.getCurrentTerm());
        heartbeat = scheduleTimer((int) HEARTBEAT_INTERVAL, mID);
      }
    }
  }
}
