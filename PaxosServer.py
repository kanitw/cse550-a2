import sys
import SocketServer
from message import *
from datetime import datetime

class PaxosServer(SocketServer.ThreadingMixIn, SocketServer.TCPServer):
  timeout = 5

  daemon_threads = True
  allow_reuse_address = True

  def __init__(self, node_id, nodes_count, RequestHandlerClass):
    self.node_id = int(node_id)
    self.nodes_count = nodes_count
    self.proposal_queue = []
    self.current_leader_id = 1 # 1 by default
    self.leader_last_seen = datetime.now()

    # instance based object -- should be clean every new instnace round
    self.largest_accepted_proposal = (-1, None)
    self.promise_count = {}
    self.acceptance_count = {}

    # Persistent objects
    self.n = 0
    self.n_proposer = -1 # who make this server promise current n
    self.latest_executed_command = -1
    self.chosen_commands = []
    self.client_last_executed_command = {}
    self.load_state()
    self.lock_owners = {}
    self.lock_queues = {}

    server_address = ("localhost", 9000+node_id)

    SocketServer.TCPServer.__init__(self, server_address, RequestHandlerClass)

  def handle_timeout(self):
    self.check_timestamp()
    # print 'Timeout!'

  def send_to_server(self, server_id, msg_type, params):
    msg = params.copy()
    params["type"] = msg_type
    params["server_id"] = self.node_id

    self.log("send_to_server %s: %s" % (server_id, msg))
    # FIXME(kanitw): Shih-wen please finish this method
    pass

  def send_to_client(self, client_id, msg_type, params):
    msg = params.copy()
    params["type"] = msg_type
    params["server_id"] = self.node_id

    self.log("send_to_client %s: %s" % (client_id, msg))
    # FIXME(kanitw): Shih-wen please finish this method
    pass

  def log(self, log):
    print "Server %s: %s" % (self.server_id, log)

  def in_proposal_queue(self, msg):
    for m in self.proposal_queue:
      if m["client_id"] == msg["client_id"] and m["client_command_id"] == msg["client_command_id"]:
        return True
    return False

  def is_executed(self, client_id, client_command_id):
    if client_id in self.client_last_executed_command:
      if client_command_id <= self.client_last_executed_command[client_id]:
        return True
    return False

  # send message to all nodes except itself
  def broadcast(self, msg_type, params):
    for node in range(self.nodes_count):
      if node != self.node_id:
        # for all nodes other than this one!
        self.send_to_server(node, msg_type, params)

  def get_v(self, client_msg):
    return {
      "client_id": client_msg["client_id"],
      "client_command_id": client_msg["client_command_id"],
      "command": client_msg["command"]
    }

  def broadcast_prepare(self):
    msg = self.proposal_queue[0]
    self.broadcast(PREPARE_REQUEST,{
      "n": self.n
      # kanitw: hide because we didn't use
      # ,
      # "v": self.get_v(msg)
    })

  def broadcast_accept(self, n, v):
    self.broadcast(ACCEPT_REQUEST, {
      "n": n,
      "v": v
    })

  def broadcast_execute(self, params):
    self.broadcast(EXECUTE, params)

  def inc_count(self, counter, n):
    if not n in counter:
      counter[n] = 1
    else:
      counter[n] += 1


  def get_n_tuple(self):
    return [self.n, self.node_id]

  def get_msg_n_tuple(self, msg):
    return [msg["n"], msg["server_id"]]

  ## compare tuple of n  (n, node_id)
  @staticmethod
  def compare_n_tuples(nt1, nt2):
    if nt1[0]-nt2[0] == 0:
      return nt1[1] - nt2[1]
    return nt1[0]-nt2[0]

  def save_state(self):
    pass
    #Professor Arvind says we don't have to handle the recovery case
    #therefore we won't implement this method
    #save the following
    #self.n = 0
    #self.chosen_commands = []
    #self.latest_executed_command = -1
    #self.n_proposer ?? ... do we really need this?
    #self.client_executed_command_map = {}
    #self.lock_owners
    #self.lock_queues


  def load_state(self):
    pass
    #load everything we save
    # since we don't have to deal with recovery in this homework, we ignore this method.


  def reset_instance(self):
    self.largest_accepted_proposal = (-1, None)
    self.promise_count = {}
    self.acceptance_count = {}
    self.n_proposer = -1
    self.n = 0

  def execute(self, v):
    #v(client_id, client_command_id, command)
    (action, var) = v["command"].split("_")
    client_id = v["client_id"]
    client_command_id = v["client_command_id"]

    if action == "lock":
      if not var in self.lock_owners:
        self.lock_owners[var] = client_id
      elif self.lock_owners[var] != client_id:
        # there is an owner and the requested client is not the owner
        self.lock_queues.setdefault(var, []).push((client_id, client_command_id))
    elif action == "unlock":
      if len(self.lock_queues.setdefault(var, [])) > 0:
        # assign the lock to the new owner and send executed to him
        (new_client_id, new_client_command_id) = self.lock_queues[var].pop(0)
        self.lock_owners[var] = new_client_id
        self.send_to_client(new_client_id, EXECUTED, {
          "client_command_id": new_client_command_id
        })
      else:
        self.lock_queues.pop(var, None) #just remove the lock

  def handle_execute_msg(self, params):
    # params include (instance, n, v(client_id, client_command_id, command))

    instance = params["instance"]
    v = params["v"]

    while len(self.chosen_commands) < instance:
      self.chosen_commands.push(None) #push empty slot just in case

    if instance == self.latest_executed_command + 1:
      # the next command to execute, do it right away
      if instance == len(self.chosen_commands):
        self.chosen_commands.push(v)
      else:
        self.chosen_commands[instance] = v

      i = instance
      while i < len(self.chosen_commands) and self.chosen_command[i] != None:
        v_to_exec = self.chosen_commands[i]
        self.execute(v_to_exec)
        client_id = v_to_exec["client_id"]
        client_command_id = v_to_exec["client_command_id"]
        self.client_last_executed_command[client_id] = client_command_id
        self.latest_executed_command = i
        self.save_state()
        i+=1

      self.reset_instance() # TODO(kanitw): should this be in the while loop?
      if self.node_id == self.current_leader_id:
        self.proposal_queue.pop(0) # remove latest proposed
        # start proposing next one - broadcast_prepare method will take care of this
        self.broadcast_prepare()

    elif instance > self.latest_executed_command + 1:
      # newer command ... maybe old instance command is missing
      # TODO: ask the leader PLEASE_UPDATE_ME
      # TODO: special case if the leader ask someone else
      pass

    else:
      pass # ignore old instance

  def message_handler(self, msg):
    self.log("receive msg: %s" % msg)

    client_id = msg.get("client_id")  # id of message sender if it's a message from a client
    server_id = msg.get("server_id")  # id of message sender if it's a message from a server

    if "instance" in msg:
      instance = msg["instance"]

      # ignore old message
      if instance < self.latest_executed_command:
        return
      # if future message arrive
      if instance > self.latest_executed_command:
        #TODO(kanitw): send leader PLEASE_UPDATE_ME
        return
        #FUTUREWORK should we handle multiple instances at the same time?
        # if we do handle multiple instances, this can be thrown away
        # ignore future message already resolved
        #if instance in self.chosen_commands and self.chosen_commands[instance] != None:
        #  return


    ### messages for PROPOSER

    if msg["type"] == CLIENT_REQUEST:
      # CLIENT_REQUEST(client_id, client_command_id, command)

      client_command_id = msg["client_command_id"]
      if self.node_id != self.current_leader_id:
        self.send_to_client(client_id, PLEASE_ASK_LEADER, {"current_leader_id": self.current_leader_id})
      elif self.in_proposal_queue(msg):
        pass  # ignore
      elif self.is_executed(client_id, client_command_id):
        self.send_to_client(client_id, EXECUTED, {"client_command_id": client_command_id})
      else:
        self.proposal_queue.append(msg)
        if len(self.proposal_queue) == 1:  # so it was empty before
          self.broadcast_prepare()


    if msg["type"] == PREPARE_AGREE:
      # PREPARE_AGREE(instance, n, largest_accepted_proposal(n,cmd))

      msg_largest_accepted_proposal = msg["largest_accepted_proposal"]

      if msg_largest_accepted_proposal is not None and \
        msg_largest_accepted_proposal[0] > self.largest_accepted_proposal[0]:
        self.largest_accepted_proposal = msg_largest_accepted_proposal

      self.inc_count(self.promise_count, msg["n"])

      # check if a majority has agreed
      if self.promise_count[msg["n"]] + 1 == self.nodes_count/2 + 1:
        # 1 on the left side = the leader itself!
        # and we only broadcast only the first time it has the majority to vote on something

        # (from PMS) issue a proposal with number n and value v, where v is the value of the highest-numbered proposal
        # among the responses, or is any value selected by the proposer if the responders reported no proposals.
        if self.largest_accepted_proposal is not None:
          self.broadcast_accept(self.largest_accepted_proposal[0], self.largest_accepted_proposal[1])
        else:
          self.broadcast_accept(msg["n"], self.get_v(self.proposal_queue[0]))

    if msg["type"] == PREPARE_REJECT:
      # PREPARE_REJECT(instance, n, min_n)
      # n is the rejected n, min_n is min_n that the acceptor will accept

      # abandon all proposal less with number < min_n
      # but we propose one at a time so we only have to remove one

      # no need to use compare_n_tuples
      if msg["min_n"] < self.n:
        pass  # this is a rejection for abandoned message
      else:
        # first time to get rejection for message n
        self.n = msg["min_n"] + 1
        self.broadcast_prepare()


    ### messages for ACCEPTOR:
    # Note: Acceptor must remember its highest promise for each command instance.

    if msg["type"] == PREPARE_REQUEST:
      # PREPARE_REQUEST (server_id, n):
      self.leader_last_seen = datetime.now()

      # FUTUREWORK - is there a case that leader is not the sender?

      if PaxosServer.compare_n_tuples(self.get_msg_n_tuple(msg), [self.n, self.n_proposer]) >= 0:
        self.n = msg["n"]
        self.n_proposer = server_id
        self.save_state()

        self.send_to_server(server_id, PREPARE_AGREE, {
          "n": msg["n"],
          "largest_accepted_proposal": self.largest_accepted_proposal
        })
      else:
        self.send_to_server(server_id, PREPARE_REJECT, {
          "n": msg["n"],
          "min_n": self.n
        })


    if msg["type"] == ACCEPT_REQUEST:
      # ACCEPT_REQUEST(instance, server_id, n, v(client_id, client_command_id, command))
      self.leader_last_seen = datetime.now()

      if PaxosServer.compare_n_tuples(self.get_msg_n_tuple(msg), self.get_n_tuple()) >= 0:
        self.send_to_server(server_id, ACCEPT, {
          "n": msg["n"],
          "v": msg["v"]
        })
        self.largest_accepted_proposal = (msg["n"], msg["v"])

      else:
        pass
        #NICETODO: Maybe it's nice to notify proposer in this case?


    ### messages for DISTINGUISHED_LEARNERS
    if msg["type"]==ACCEPT:
      #ACCEPT(instance, n, v(client_id, client_command_id, command))
      client_id = msg["v"]["client_id"]
      client_command_id = msg["v"]["client_command_id"]

      self.inc_count(self.acceptance_count, self.n)
      if self.acceptance_count[self.n] + 1 == self.nodes_count/2 + 1:
        # 1=itself!
        # and we want to broadcast the execute only once

        params = msg.copy()
        params["instance"] = self.latest_executed_command + 1

        self.broadcast_execute(params)
        self.handle_execute_msg(params)
        self.send_to_client(client_id, EXECUTED, {
          "client_command_id": client_command_id
        })


    ### messages for LEARNERS
    if msg["type"] == EXECUTE:
      # since dlearner and leader is the same node
      self.leader_last_seen = datetime.now()

      # just run execute method (so it behaves similar to the d-learner.
      self.handle_execute_msg(msg)

    if msg["type"] == ARE_YOU_AWAKE:
      self.send_to_server(server_id, IM_AWAKE) #TODO do we need any param?

    if msg["type"] == PLEASE_UPDATE_ME:
      # we only send PLEASE_UPDATE_ME to the leader
      if self.node_id == self.current_leader_id:

        self.log("OKAY WE NEED PLEASE_UPDATE_ME!!!")
        # TODO(kanitw): do we really need this case
        # if yes, send data back
        # and think about what if the leader doesn't know
      else:
        pass

    self.check_timestamp()

  def check_timestamp(self):
    pass
    #FIXME(kanitw):
    #if(datetime.now() - self.leader_last_seen < MAX_TIMEOUT){
    #    send ARE_YOU_ALIVE message to the current leader
    #    if timeout:
    #      #TODO elect leader
    #}

