import socket, sys
import httplib
import pyodbc
import json
import types
import datetime
import urllib2
import os
import logging
import re, time
from logging import info, warning, debug

#logging.basicConfig(level=logging.DEBUG)


class GerritDataException(Exception):
  pass

def main(project):

 
  config = AndroidGerritConfig()

  
  connStr = ('DRIVER={SQL Server};SERVER=gerrit;DATABASE=Gerrit;UID=user;PWD=user')
  
  
  connection = pyodbc.connect(config.ConnectionString)
  cursor = connection.cursor()
  
  createTables(cursor)

  gm = GerritMiner(config)

  recordedReviews = set()
  sql = "select ReviewId from review"
  for row in cursor.execute(sql).fetchall():
    recordedReviews.add(row.ReviewId)

  print recordedReviews
  rl = gm.GetReviewList()
  
  reviewId = config.StartingReviewNumber
  while reviewId > 0:
    if not reviewId in recordedReviews:
      recordReview(cursor, reviewId, gm)
    print "recording review", reviewId
    reviewId -= 1


def recordReview(cursor, reviewId, gerritMiner):
  changeDetailsJson = gerritMiner.GetChangeDetails(reviewId)
  print changeDetailsJson
  if changeDetailsJson.has_key("error"):
    return
  changeDetails = ChangeDetails(changeDetailsJson)
  changeDetails.ExecuteInsert(cursor)
  #print changeDetails

  for comment in changeDetails.Comments:
    print comment
    comment.ExecuteInsert(cursor)

  for person in changeDetails.People:
    print person
    person.ExecuteInsert(cursor)

  for approval in changeDetails.Approvals:
    print approval
    approval.ExecuteInsert(cursor)
    
  for patchNum in changeDetails.PatchSetNumbers:
    patchSetDetailsJson = gerritMiner.GetPatchSetDetails(reviewId, patchNum)
    if patchSetDetailsJson.has_key("error"):
            continue
    #print patchSetDetailsJson
    patchSet = PatchSet(patchSetDetailsJson)
    patchSet.ExecuteInsert(cursor)
    print patchSet
    for patchSetFile in patchSet.Files:
      try: 
        print patchSetFile
      except:
        pass
      patchSetFile.ExecuteInsert(cursor)
      if patchSetFile.NumberOfComments > 0:
        patchFileBodyJson = gerritMiner.GetPatchFileBody(reviewId, patchNum, patchSetFile.path)
        patchSetFileBody = PatchSetFileBody(json.loads(patchFileBodyJson))
        for comment in patchSetFileBody.Comments:
          print comment
          comment.ExecuteInsert(cursor)
                      
  cursor.commit() 
  return


def createTables(cursor, drop = False):
  if drop:
    cursor.execute("""IF object_id('Review', 'U') is not null
                       DROP TABLE Review""")
    cursor.commit()

  cursor.execute("""IF object_id('Review', 'U') is not null
   PRINT 'TABLE PRESENT'
   ELSE
   CREATE TABLE Review
   (
      ReviewId int,
      OwnerId int,
      Message nvarchar(max),
      Subject nvarchar(max),
      CreatedOn datetime,
      NumberOfPatches int,
      LastUpdatedOn datetime,
      Project nvarchar(max),
      Branch nvarchar(max),
      SubmitType nvarchar(max),
      Status nvarchar(20)
    ); """)

  cursor.commit()

  if drop:
    cursor.execute("""IF object_id('Comment', 'U') is not null
                       DROP TABLE Comment""")
    cursor.commit()

  cursor.execute("""IF object_id('Comment', 'U') is not null
                     PRINT 'TABLE PRESENT'
                     ELSE
                     CREATE TABLE Comment
           (
              ReviewId int,
              PatchSetId nvarchar(max),
              CommentId nvarchar(max),
	            Message nvarchar(max),
              WrittenOn datetime,
              AuthorId int,
	            Path nvarchar(max),
	            LineNumber int,
              Side varchar(10)
          )""")
  cursor.commit()

  if drop:
    cursor.execute("""IF object_id('PatchSet', 'U') is not null
                       DROP TABLE PatchSet""")
    cursor.commit()

  cursor.execute("""IF object_id('PatchSet', 'U') is not null
                     PRINT 'TABLE PRESENT'
                     ELSE
                     CREATE TABLE PatchSet
           (
              ReviewId int,
              PatchSetId varchar(max),
              PatchSetNumber int,
              NumberOfFiles int,
              CreatedOn datetime,
              GitRevision varchar(max)
      ) """)
  cursor.commit()
  
  
  if drop:
    cursor.execute("""IF object_id('File', 'U') is not null
                       DROP TABLE PatchSetFile""")
    cursor.commit()

  cursor.execute("""IF object_id('PatchSetFile', 'U') is not null
     PRINT 'TABLE PRESENT'
     ELSE
     CREATE TABLE PatchSetFile
     (
        ReviewId int,
        PatchSetId varchar(max),
        PatchSetFileId nvarchar(max),
        PatchNumber int,
        FileId nvarchar(max),
        Path nvarchar(max),
        ChangeType nvarchar(10),
        LinesAdded int,
        LinesDeleted int
      ) """)
  cursor.commit()
  
  if drop:
    cursor.execute("""IF object_id('Person', 'U') is not null
                       DROP TABLE Person""")
    cursor.commit()

  cursor.execute("""IF object_id('Person', 'U') is not null
                     PRINT 'TABLE PRESENT'
                     ELSE
                     CREATE TABLE Person
           (
              PersonId int,
              Name nvarchar(255),
              Email nvarchar(255)
      ) """)
  cursor.commit()
  
  if drop:
    cursor.execute("""IF object_id('Approval', 'U') is not null
      DROP TABLE Approval""")
    cursor.commit()

  cursor.execute("""IF object_id('Approval', 'U') is not null
     PRINT 'TABLE PRESENT'
     ELSE
     CREATE TABLE Approval
     (
        ReviewId int,
        PersonId int,
        ReviewedStatus int,
        ReviewedWhen datetime,
        VerifiedStatus int,
        VerifiedWhen datetime
      )""")
  cursor.commit()
  
  return

class GerritConfig:
  def GetHost(self):
    raise Exception("Not implemented")

class AndroidGerritConfig(GerritConfig):
  def __init__(self):
    self.StartingReviewNumber = 51750
    self.ConnectionString = "DRIVER={SQL Server};SERVER=CBIRD-MBP;DATABASE=gerrit;UID=gerritminer;PWD=Davis123"

  def GetHost(self):
    return "android-review.googlesource.com"

class ChromeGerritConfig(GerritConfig):
  def __init__(self):
    self.StartingReviewNumber = 0 # Fill this in
    self.ConnectionString = 'DRIVER={SQL Server};SERVER=MURTUZA-PC;DATABASE=ChromeOrSomething;UID=user3;PWD=user3'

  def GetHost(self):
    return "FILL THIS IN"


class GerritMiner:

  def __init__(self, config):
    self.Config = config
    self.cacheDir = "json"
  
  def GetPatchFileBody(self,id,pnum,filename):
      jsonFilename = "%d-%d-%s.json" % (id, pnum, re.sub("[^a-zA-Z0-9]", "_", filename))
      url = "/gerrit_ui/rpc/PatchDetailService"
      req = {"jsonrpc" : "2.0", 
        "method": "patchScript",
        "params": [{"fileName": filename,"patchSetId":{"changeId":{"id":id},"patchSetId":pnum}},None,{"changeId":{"id":id},
                       "patchSetId":pnum},
                       {"context":10,"expandAllComments":False,"ignoreWhitespace":"N","intralineDifference":True,
                               "lineLength":100,"manualReview":False,"retainHeader":False,"showLineEndings":True,
                               "showTabs":True,"showWhitespaceErrors":True,"skipDeleted":False,"skipUncommented":False,
                               "syntaxHighlighting":True,"tabSize":8}
                     ]
        }
      reqJson = json.dumps(req)
      data = self.GetRequestOrCached(url,"POST", reqJson, jsonFilename)
      js = json.loads(data)
      if js.has_key("error"):
        os.unlink(os.path.join(self.cacheDir, jsonFilename))
        raise GerritDataException("Error getting url %s\nmessage: %s" % (url, js["error"]["message"]))
      return data
  
  def GetPatchSetDetails(self,id,pnum):
      filename = "%d-%d-PatchDetails.json" % (id, pnum)
      url = "/gerrit_ui/rpc/ChangeDetailService"
      req = {"jsonrpc" : "2.0", 
        "method": "patchSetDetail2",
        "params": [None,{"changeId":{"id":id},"patchSetId":pnum},{"context":10,"expandAllComments":False,"ignoreWhitespace":"N","intralineDifference":True,"lineLength":100,"manualReview":False,"retainHeader":False,"showLineEndings":True,"showTabs":True,"showWhitespaceErrors":True,"skipDeleted":False,"skipUncommented":False,"syntaxHighlighting":True,"tabSize":8}],
        }
      data = self.GetRequestOrCached(url, "POST", json.dumps(req), filename)
      return json.loads(data)


  def GetChangeDetails(self, id):
      filename = "%d-ChangeDetails.json" % id
      url = "/gerrit_ui/rpc/ChangeDetailService"
      req = {"jsonrpc" : "2.0", 
        "method": "changeDetail",
        "params": [{"id": id}],
        "id": 44
        }
      data = self.GetRequestOrCached(url, "POST", json.dumps(req), filename)
      return json.loads(data)

  def GetReviewList(self):
    data = {"jsonrpc": "2.0",
        "method": "allQueryNext",
        "params": ["status:reviewed", "z", 100],
        "id": 1
        }
    url = "/changes/?q=status:reviewed&n=500&O=1"
    data = self.GetRequestOrCached(url, "GET", "", "ChangeList.json")
    js = json.loads(data)
    return ReviewList(js)

  def GetRequestOrCached(self, url, method, data, filename):
    path = os.path.join(self.cacheDir, filename)
    if not os.path.exists(path):
      data = self.MakeRequest(url, method, data)
      time.sleep(1)
      data = data.replace(")]}'", "")
      f = open(path, "w")
      f.write(data)
      f.close()
    return open(path).read()  

  def MakeRequest(self, url, method, data, port=443):
    successful = False
    while not successful:
      try:
        conn = httplib.HTTPSConnection(self.Config.GetHost(), port)
        headers = {"Accept": "application/json,application/jsonrequest",
          "Content-Type": "application/json; charset=UTF-8",
          "Content-Length": len(data)}
        conn.request(method, url, data, headers)
        successful = True
      except socket.error as err:
        # this means a socket timeout
        if err.errno != 10060:
          raise(err)
        else:
          print err.errno, str(err)
          print "sleep for 1 minute before retrying"
          time.sleep(60)
      
    resp = conn.getresponse()
    if resp.status != 200:
            raise GerritDataException("Got status code %d for request to %s" % (resp.status, url))
    return resp.read()



class JSONLookup(object):
  JSONFields = []
  JSONFieldMapping = {}

  def __init__(self, jsonData, changeDetails=None):
    self.json = jsonData
    self.changeDetails = changeDetails
    if self.__class__ == ChangeDetails:
      self.changeDetails = self

  def __getattr__(self, name):
    name = name.lower()
    debug("getting %s", name)
    if name in self.JSONFieldMapping.keys():
      path = self.JSONFieldMapping[name]
      debug("path is %s", path)
      parts = path.split("/")
      
      obj = self.json
      #print obj
      
      for part in parts:
        debug("part is %s", part)
        debug("obj is %s", obj)
        obj = obj[part]
        
      return self.FixType(obj)
    if name in self.JSONFields:
      return self.FixType(self.json[name])
      
    raise AttributeError("%s has no json attribute for '%s'" % (self.__class__.__name__, name))

  def get(self, jsPath):
    debug("get called with path %s", jsPath)
    parts = jsPath.split("/")
    debug("parts %s", parts)
    obj = self.json
    for part in parts:
      debug("in get part is %s" % part)
      if type(obj) == list:
        obj = obj[int(part)]
      elif type(obj) == dict:
        obj = obj[part]
      else:
        raise GerritDataException("can't get index %s of json object of type %s" % (str(part), str(type(obj))))

    return self.FixType(obj)


  #return true if the current object's json has the path passed in
  #and false otherwise
  def has(self, jsPath):
    parts = jsPath.split("/")
    obj = self.json
    for part in parts:
      if obj.has_key(part):
        obj = obj[part]
      else:
        return False
    return True
  
  def FixType(self, obj):
    if type(obj) in types.StringTypes:
      try:
        fmt = "%Y-%m-%d %H:%M:%S"
        dateString = obj.split(".")[0]
        dateObject = datetime.datetime.strptime(dateString, fmt)
        print "got a date string", obj, dateObject
        return dateObject
      except ValueError:
        pass
    return obj

class SQLInsertMixin:
  SQLFields = ["a", "b", "c"]
  SQLTableName = "Foo"

  def GetInsertStatement(self):
    fieldString = ", ".join(self.SQLFields)
    valueString = ", ".join(['?' for field in self.SQLFields]) 
    sql = "INSERT INTO %s (%s) VALUES (%s)" % (self.SQLTableName, fieldString, valueString)
    debug(sql)
    return sql

  def GetInsertValues(self):
    return [getattr(self, field) for field in self.SQLFields]

  def ExecuteInsert(self, cursor):
    sql = self.GetInsertStatement()
    vals = self.GetInsertValues()
    print sql
    print vals
    return cursor.execute(sql, vals)

  def FormatType(self, obj):
    # we only have strings, ints, and datetimes...
    if obj == None:
      return "NULL"
    if type(obj) == types.IntType:
      return str(obj)
    if type(obj) in types.StringTypes:
      return "'" + obj + "'" 
    if type(obj) == datetime.datetime:
      fmt = "%Y-%m-%d %H:%M:%S"
      return "'" + datetime.datetime.strftime(obj, fmt) + "'"
    print type(obj)
    raise GerritDataException("don't know how to convert to DB format:" + str(obj))

class TestSQLInsertMixin(SQLInsertMixin):
  SQLFields = ["a", "b", "cool", "date"]
  SQLTableName = "Foo"

  def __init__(self):
    self.a = 5
    self.b = "hello there"
    self.date = datetime.datetime.now()
    self.cool = None

  def execute(self, string, values):
    print string, ":", values
  
class ReviewList:
  def __init__(self, jsonData):
    self.json = jsonData 

  def Items(self):
    return [ReviewListItem(x) for x in self.json]
    
class ReviewListItem(JSONLookup):
  JSONFields = ["status"]
  JSONFieldMapping = {"id" : "_number"}

  def __repr__(self):
    return "%s" % self.Id


class ChangeDetails(JSONLookup, SQLInsertMixin):

  JSONFieldMapping = {
      "reviewid":"result/change/changeId/id",
      "ownerid": "result/change/owner/id",
      "message": "result/currentDetail/info/message",
      "subject": "result/change/subject",
      "createdon": "result/change/createdOn",
      "numberofpatches":"result/currentPatchSetId/patchSetId",
      "lastupdatedon": "result/change/lastUpdatedOn",
      "project":"result/change/dest/projectName/name",
      "branch":"result/change/dest/branchName",
      "submittype":"result/submitTypeRecord/type" ,              
      }
  
  SQLTableName = "Review"
  SQLFields = ["ReviewId", "ownerid", "message", "subject", "createdon",
    "numberofpatches", "lastupdatedon", "project", "branch", "submittype", "Status"]

  @property
  def Status(self):
    statLetter = self.get("result/change/status").lower()
    return {"n": "Open", "m": "merged", "a": "Abandoned"}.get(statLetter, statLetter)

  @property
  def People(self):
    people = []
    for account in self.get("result/accounts/accounts"):
      if account.has_key("fullName"):
        people.append(Person(account))
    return people

  @property
  def Comments(self):
    return [Comment(x) for x in self.get("result/messages")]

  @property
  def Approvals(self):
    approvals = []
    for js in self.get("result/approvals"):
      approval = Approval(js)
      approval.SetReviewId(self.ReviewId)
      approvals.append(approval)
    return approvals

  @property
  def PatchSetNumbers(self):
    patchSetNumbers = []
    for js in self.get("result/patchSets"):
      patchSetNumbers.append(js["id"]["patchSetId"])
    return patchSetNumbers


class Person(JSONLookup, SQLInsertMixin):
  JSONFieldMapping = {"name": "fullName", "personid":"id/id"}

  SQLTableName = "Person"
  SQLFields = {"Name", "Email", "PersonId"}

  @property
  def Email(self):
    if self.has("preferredEmail"):
      return self.get("preferredEmail")
    return None

  def ExecuteInsert(self, cursor):
    vals = self.GetInsertValues()
    sql = "If not exists (select * from Person where PersonId = %d ) begin %s end" % (self.PersonId, self.GetInsertStatement()) 
    print sql
    print vals
    return cursor.execute(sql, vals)

  def __repr__(self):
    return "< Person %d >" % self.PersonId

class Approval(JSONLookup, SQLInsertMixin):
  JSONFieldMapping = {"personid": "account/id"}

  SQLTableName = "Approval"
  SQLFields = ["ReviewId", "PersonId", "ReviewedStatus", "ReviewedWhen",
      "VerifiedStatus", "VerifiedWhen"]

  def SetReviewId(self, reviewId):
    self._ReviewId = reviewId

  @property
  def ReviewId(self):
    return self._ReviewId

  @property
  def ReviewedStatus(self):
    for app in self.json["approvals"]:
      if app["key"]["categoryId"]["id"] == "CRVW":
        return app["value"]
    return None

  @property
  def ReviewedWhen(self):
    for app in self.json["approvals"]:
      if app["key"]["categoryId"]["id"] == "CRVW":
        return self.FixType(app["granted"])
    return None

  @property
  def VerifiedStatus(self):
    for app in self.json["approvals"]:
      if app["key"]["categoryId"]["id"] == "VRIF":
        return app["value"]
    return None
  
  @property
  def VerifiedWhen(self):
    for app in self.json["approvals"]:
      if app["key"]["categoryId"]["id"] == "VRIF":
        return self.FixType(app["granted"])
    return None

class PatchSet(JSONLookup, SQLInsertMixin):
  JSONFieldMapping={"createdon": "result/patchSet/createdOn",
                  "reviewid": "result/info/key/changeId/id",
                  "patchsetnumber": "result/patchSet/id/patchSetId",
                  "gitrevision": "result/patchSet/revision/id"
                  }
 
  SQLTableName = "PatchSet"
  SQLFields = [ "ReviewId", "PatchSetId", "PatchSetNumber", 
      "NumberOfFiles", "CreatedOn", "GitRevision"]
  
  @property
  def PatchSetId(self):
    return "%d-%d" % (self.get("result/info/key/changeId/id"), self.get("result/info/key/patchSetId"))

  @property
  def NumberOfFiles(self):
    return len(self.get("result/patches"))

  @property
  def Files(self):
    return [PatchSetFile(js) for js in self.get("result/patches")]

  def __repr__(self):
    return "<PatchSet %s>" % self.PatchSetId

class PatchSetFile(JSONLookup, SQLInsertMixin):
  JSONFieldMapping={"linesdeleted": "deletions",
                  "linesadded": "insertions",
                  "path": "key/fileName",
                  "numberofcomments": "nbrComments",
                  "patchnumber": "key/patchSetId/patchSetId"
                  }

  SQLTableName = "PatchSetFile"
  SQLFields = ["ReviewId", "PatchSetId", "PatchSetFileId", "PatchNumber",
      "Path", "ChangeType", "LinesAdded", "LinesDeleted"]

  @property
  def ReviewId(self):
    return self.get("key/patchSetId/changeId/id")
  
  @property
  def PatchSetId(self):
    return "%d-%d" % (self.get("key/patchSetId/changeId/id"), self.get("key/patchSetId/patchSetId"))

  @property
  def PatchSetFileId(self):
    return "%d-%d-%s" % (self.get("key/patchSetId/changeId/id"), self.get("key/patchSetId/patchSetId"), self.path)

  @property
  def ChangeType(self):
    return {"A": "added", "M": "modified", "R": "removed", "D": "deleted", "C": "c"}[self.get("changeType")]

  def __repr__(self):
    return "< PatchSetFile %s >" % self.PatchSetFileId


class PatchSetFileBody(JSONLookup):
  JSONFieldMapping={}

  #def Comments(self):
  @property
  def Comments(self):
    c = [Comment(js) for js in self.get("result/comments/a")]
    c.extend([Comment(js) for js in self.get("result/comments/b")])
    return c
    
class Comment(JSONLookup, SQLInsertMixin):
  JSONFieldMapping = {"message": "message",
    "writtenon": "writtenOn",
    }

  SQLTableName = "Comment"
  SQLFields = ["ReviewId", "PatchSetId", "CommentId",
    "Message", "WrittenOn", "AuthorId", "Path", "LineNumber", "Side"]


  @property
  def CommentId(self):
    return "%s-%s" % (self.PatchSetId, self.get("key/uuid"))

  @property
  def AuthorId(self):
    if self.has("author/id"):
      return self.get("author/id")
    return None

  @property
  def PatchSetId(self):
    if self.has("key/patchKey/fileName"):
      return "%d-%d" % (
        self.get("key/patchKey/patchSetId/changeId/id"),
        self.get("key/patchKey/patchSetId/patchSetId"),
      )
    elif self.has("patchset"):
      return "%d-%d" % (
        self.get("patchset/changeId/id"),
        self.get("patchset/patchSetId"),
      )
    else:
      return "%d-0" % self.get("key/changeId/id")

  
  @property
  def ReviewId(self):
    if self.has("key/patchKey/fileName"):
        return self.get("key/patchKey/patchSetId/changeId/id")
    else:
        return self.get("key/changeId/id")

  @property
  def Path(self):
    if self.has("key/patchKey/fileName"):
      return self.get("key/patchKey/fileName")
    return None

  @property
  def LineNumber(self):
    if self.has("lineNbr"):
      return self.get("lineNbr")
    return None

  @property
  def Side(self):
    if self.has("side"):
      return ["original", "modified"][self.get("side")]
    return None

  def __repr__(self):
    return "< Comment %s >" % self.CommentId
  
  def __str__(self):
    return "< Comment %s >" % self.CommentId

    

if __name__ == "__main__":
  main(sys.argv[1])




  
