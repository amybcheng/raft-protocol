package edu.duke.raft;
import java.util.*;

public class FollowerMode extends RaftMode {
  private Timer myTimer;
  private String me = "FOLLOWER";

  public void go () {
    synchronized (mLock) {
      
      int term = mConfig.getCurrentTerm();
      mConfig.setCurrentTerm(term, 0);//say we haven't voted yet in this term     
      // to set a Timer for 1 second w/ ID=mID
      Random rand = new Random(); 
      int random_time = rand.nextInt(ELECTION_TIMEOUT_MAX)+ELECTION_TIMEOUT_MIN;
      //int random_time = mConfig.getTimeoutOverride();
       //System.out.println(mID +"'s timeout time: "+ random_time);
      myTimer = scheduleTimer((int)random_time, mID);
      
      System.out.println ("S" + 
        mID + 
        "." + 
        term + 
        ": switched to follower mode.");
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
      int term = mConfig.getCurrentTerm ();
      int lastIndex = mLog.getLastIndex();
      int lastTerm = mLog.getLastTerm();
      int lastVote = mConfig.getVotedFor();

      if(term < candidateTerm)//advance term but dont vote for 
      {
          mConfig.setCurrentTerm(candidateTerm, 0);
          return term;
      }
      
      if (candidateTerm < term || // if candidate is behind follower in term 
         (lastVote != 0 && term == candidateTerm && lastVote != candidateID) || //already voted in given term and not for candidate
         (lastTerm > lastLogTerm) || //followers log has a higher lastTerm
         (lastTerm == lastLogTerm && lastIndex > lastLogIndex)//follower has same last term and longer log
         )
      { 
        return term;//don't vote for candidate
      }        
      else//vote for candidate
      { 
        myTimer.cancel();//reset timer since we're voting for a valid candidate 
        Random rand = new Random(); 
        int random_time = rand.nextInt(ELECTION_TIMEOUT_MAX)+ELECTION_TIMEOUT_MIN;
        myTimer = scheduleTimer(random_time, mID);
        mConfig.setCurrentTerm(term, candidateID);
        return 0;
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
  public int appendEntries (int leaderTerm,  //if follower learns log entry committed,
          int leaderID,                      // apply to state machine
          int prevLogIndex,  //if entries have same index and term, store same command
          int prevLogTerm,   //if logs match in index and term, all preceding entries match
          Entry[] entries,
          int leaderCommit) { //log entry: term number of leader when received, command,
    synchronized (mLock) {    //highest committed index --> one index + term entry per command
      myTimer.cancel();
      int term = mConfig.getCurrentTerm ();
      int lastIndex = mLog.getLastIndex();
      int lastTerm = mLog.getLastTerm();
      //int lastRound = RaftResponses.getRounds(leaderTerm)[mID + 1];
      int ret=-1;

      //System.out.println(mID + "." + term + " received appendEntries from " + leaderID + "." + leaderTerm);
      if(leaderCommit < lastIndex)//we have uncommited entires 
      {
        return mConfig.getCurrentTerm();
      }
      if (leaderTerm > term){ // if the leader is further along than us, update our term
        int voted = mConfig.getVotedFor();
        mConfig.setCurrentTerm(leaderTerm,0);
        //ret=0;
        // do we set ret=0 here?
      }
      if (prevLogIndex == 0){ //accept all entries if is completely wrong. nextIndex[me]=0
        mLog.insert(entries,prevLogIndex-1,prevLogTerm);
        ret=0;
      }
      else if (leaderTerm < term){
        ret=mConfig.getCurrentTerm();
      }
      else if (lastIndex != prevLogIndex-1){ // if my last index != nextIndex -1
        ret=mConfig.getCurrentTerm();
      }
      else if (lastTerm != prevLogTerm){
        ret=mConfig.getCurrentTerm();
      }
      else if (mLog.insert(entries,prevLogIndex-1,prevLogTerm) == -1){
        if (prevLogIndex - 1 == lastIndex && lastTerm == prevLogTerm) { // if everything is chill in the world, all logs converged
          ret=0;
        }
        else ret=mConfig.getCurrentTerm();
      }
      else {
        ret=0; // nothing seems to be the problem?
      }

      //System.out.println(mID + "." + mConfig.getCurrentTerm() + " responded to appendEntries from " + leaderID + "." + leaderTerm + " with " + ret);
      
      Random rand = new Random(); 
      int random_time = rand.nextInt(ELECTION_TIMEOUT_MAX)+ELECTION_TIMEOUT_MIN;
      //int random_time = mConfig.getTimeoutOverride();
      myTimer = scheduleTimer(random_time, mID);


      return ret;
    }
  } //follower refuses new entry if doesn't find entry in its log with same index and term
    // leader maintains next index for each follower -- index next log entry leader 
    //must send to follower


  // @param id of the timer that timed out
  public void handleTimeout (int timerID) {
    synchronized (mLock) {
      if (timerID == mID) { // if it's our timer timing out
            // switch to candidate
            myTimer.cancel();
            //System.out.println(me + " " + mID + " timed out!");
            CandidateMode mode = new CandidateMode();
            RaftServerImpl.setMode(mode);
        } else {
            throw new RuntimeException("Oops I didn't set this timer!");
      }
    }
  }
}