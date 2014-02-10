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
    self.largest_accepted_proposal_n = -1
    self.proposal_queue = []
    self.current_leader_id = 1 # 1 by default
    self.promise_count = {}
    self.acceptance_count = {}
    self.leader_last_seen = datetime.now()

    # Persistent objects
    self.n = 0
    self.chosen_commands = []
    self.client_last_executed_command = {}
    self.latest_executed_command = -1
    self.n_proposer = -1
    self.load_s()
    self.lock_owners = {}
    self.lock_queues = {}

    server_address = ("localhost", 9000+node_id)

    SocketServer.TCPServer.__init__(self, server_address, RequestHandlerClass)

  def handle_timeout(self):
    #FIXME handle timeout
    print 'Timeout!'

  def send_to_server(self, server_id, msg_type, params):
    msg = params.copy()
    params["type"] = msg_type
    params["sender"] = self.node_id

    self.log("send_to_server %s: %s" % (server_id, msg))
    # FIXME(kanitw): Shih-wen please finish this method
    pass

  def send_to_client(self, client_id, msg_type, params):
    msg = params.copy()
    params["type"] = msg_type
    params["sender"] = self.node_id

    self.log("send_to_client %s: %s" % (client_id, msg))
    # FIXME(kanitw): Shih-wen please finish this method
    pass

  def log(self, log):
    print "Server %s: %s" % (self.server_id, log)

  def in_proposal_queue(self, msg):
    #FIXME check if (msg.client_id, msg.client_command_id0 are in queue
    pass

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

  def broadcast_prepare(self):
    msg = self.proposal_queue[0]
    self.broadcast(PREPARE_REQUEST,{
      "n_tuple": self.get_n_tuple(),
      "v": msg["command"]
    })

  def broadcast_accept(self, n, command):
    self.broadcast(ACCEPT_REQUEST, {
      "n": n,
      "v": command
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


  ## compare tuple of n  (n, node_id)
  @staticmethod
  def compare_n_tuples(nt1, nt2):
    if nt1[0]-nt2[0] == 0:
      return nt1[1] - nt2[1]
    return nt1[0]-nt2[0]

  def save_s(self):
    pass
    #FIXME(kanitw) Shih-wen says it's easy ... save the following
    #self.n = 0
    #self.chosen_commands = []
    #self.latest_executed_command = -1
    #self.n_proposer ?? ... do we really need this?
    #self.client_executed_command_map = {}
    #self.lock_owners
    #self.lock_queues


  def load_s(self):
    pass
    #FIXME(kanitw) Shih-wen says it's easy ... load what we save!

  def reset_instance(self):
    self.largest_accepted_proposal_n = None
    self.pr = 0

  def execute(self, command):
    #command(client_id, client_command_id, v)
    (action, var) = command["v"].split("_")
    client_id = command["client_id"]
    client_command_id = command["client_command_id"]

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
    # params include (instance, client_id, client_command_id, n, v)

    instance = params["instance"]
    command = {
          "client_id": params["client_id"],
          "client_command_id": params["client_command_id"],
          "v": params["v"]
        }

    while len(self.chosen_commands) < instance:
      self.chosen_commands.push(None) #push empty slot just in case

    if instance == self.latest_executed_command + 1:
      # the next command to execute, do it right away
      if instance == len(self.chosen_commands):
        self.chosen_commands.push(command)
      else:
        self.chosen_commands[instance] = command

      i = instance
      while i < len(self.chosen_commands) and self.chosen_command[i] != None:
        self.execute(self.chosen_commands[i])
        client_id = command["client_id"]
        client_command_id = command["client_command_id"]
        self.client_last_executed_command[client_id] = client_command_id
        self.save_s()
        i+=1

      self.reset_instance() # FIXME(kanitw): should this be in the while loop?
      if self.node_id == self.current_leader_id:
        self.proposal_queue.pop(0) # remove latest proposed
        # start proposing next one - broadcast_prepare method will take care of this
        self.broadcast_prepare()

    elif instance > self.latest_executed_command + 1:
      # newer command ... maybe old instance command is missing
      # FIXME: ask the leader to fixme
      # TODO: special case if the leader ask someone else
      pass

    else:
      pass # ignore old instance

  def message_handler(self, msg):
    self.log("receive msg: %s" % msg)

    if "instance" in msg:
      instance = msg["instance"]

      # ignore old message
      if instance < self.latest_executed_command:
        return
      # if future message arrive
      if instance > self.latest_executed_command:
        #TODO(kanitw): send leader PLEASE_UPDATE_ME
        return
        #TODO should we handle multiple instances at the same time?
        # if we do handle multiple instances, this can be thrown away
        # ignore future message already resolved
        #if instance in self.chosen_commands and self.chosen_commands[instance] != None:
        #  return


    ### messages for PROPOSER

    if msg["type"] == CLIENT_REQUEST:
      # CLIENT_REQUEST(client_id, client_command_id, command)
      client_id = msg["client_id"]
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
      # PREPARE_AGREE(instance, n, largest_accepted_proposal_n, largest_accepted_proposal_cmd)

      # FIXME(kanitw): use compare
      if msg["largest_accepted_proposal_n"] is not None and \
                      msg["largest_accepted_proposal_n"] > self.largest_accepted_proposal_n:
        self.largest_accepted_proposal_n = msg["largest_accepted_proposal_n"]
        self.largest_accepted_proposal_cmd = msg["largest_accepted_proposal_cmd"]

      self.inc_count(self.promise_count, msg["n"])

      # check if a majority has agreed
      if self.promise_count[msg["n"]] + 1 == self.nodes_count/2 + 1:
        # 1 on the left side = the leader itself!
        # and we only broadcast only the first time it has the majority to vote on something

        # (from PMS) issue a proposal with number n and value v, where v is the value of the highest-numbered proposal
        # among the responses, or is any value selected by the proposer if the responders reported no proposals.
        if self.largest_accepted_proposal_n is not None:
          self.broadcast_accept(self.largest_accepted_proposal_n, self.largest_accepted_proposal_cmd)
        else:
          self.broadcast_accept(msg["n"], self.proposal_queue[0]["command"])

    if msg["type"] == PREPARE_REJECT:
      # abandon all proposal less with number < min_n
      # but we propose one at a time so we only have to remove one

      if msg["n"] < self.n: #FIXME use compare
        pass  # this is a rejection for abandoned message
      else:
        # first time to get rejection for message n
        self.n = msg["min_n"] + 1
        self.broadcast_prepare()


    ### messages for ACCEPTOR:
    # Note: Acceptor must remember its highest promise for each command instance.

    if msg["type"] == PREPARE_REQUEST:
      # PREPARE_REQUEST (n_tuple, v):
      self.leader_last_seen = datetime.now()
      # TODO is there a case that leader is not the sender

      if PaxosServer.compare_n_tuples(msg["n_tuple"], [self.n, self.n_proposer]) >= 0:
        self.n = msg["n"]
        self.n_proposer = msg["sender"]
        self.save_s()

        self.send_to_server(msg["sender"], PREPARE_AGREE, {
          "n": msg["n"],
          "largest_accepted_proposal_n": self.largest_accepted_proposal_n,
          "largest_accepted_proposal_cmd": self.largest_accepted_proposal_cmd
        })
      else:
        self.send_to_server(msg["sender"], PREPARE_REJECT, {
          "n": msg["n"],
          "my_n": None #FIXME check....
        })


    if msg["type"] == ACCEPT_REQUEST:
      # ACCEPT_REQUEST(sender/proposer, n_tuple, v)
      self.leader_last_seen = datetime.now()
      if self.compare_n_tuples(msg["n_tuple"], self.get_n_tuple()) > 0:
        self.send_to_server(msg["sender"], ACCEPT, {
          "n_tuple": msg["n_tuple"] #FIXME(kanitw) add parameter required by accept below
        })
        self.largest_accepted_proposal_n = msg["n_tuple"][0]
        self.largest_accepted_proposal_cmd = msg["v"]
      else:
        pass
        #NICETODO: Maybe it's nice to notify proposer in this case?


    ### messages for DISTINGUISHED_LEARNERS
    if msg["type"]==ACCEPT:
      #comes with (instance, client_id, client_command_id, n,v)
      client_command_id = msg["client_command_id"]

      self.inc_count(self.acceptance_count, self.n)
      if self.acceptance_count[self.n] + 1 == self.nodes_count/2 + 1:
        # 1=itself!
        # and we want to broadcast the execute only once

        params = msg.copy()
        params["instance"] = self.latest_executed_command + 1

        self.broadcast_execute(params)
        self.handle_execute_msg(params)
        self.send_to_client(msg["client_id"], EXECUTED, {
          "client_command_id": client_command_id
        })


    ### messages for LEARNERS
    if msg["type"] == EXECUTE:
      # just run execute method (so it behaves similar to the d-learner.
      self.handle_execute_msg(msg)

    if msg["type"] == ARE_YOU_AWAKE:
      self.send_to_server(msg["sender"], IM_AWAKE) #TODO do we need any param?

    if msg["type"] == PLEASE_UPDATE_ME:
      # we only send PLEASE_UPDATE_ME to the leader
      if self.node_id == self.current_leader_id:
        pass
        # FIXME(kanitw): send data back
        # TODO(kanitw): think about what if the leader doesn't know
      else:
        pass

    self.check_timestamp()

  def check_timestamp():
    pass
    #FIXME(kanitw):
    #if(datetime.now() - self.leader_last_seen < MAX_TIMEOUT){
    #    send ARE_YOU_ALIVE message to the current leader
    #    if timeout:
    #      #TODO elect leader
    #}

