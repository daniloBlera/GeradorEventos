import datetime
import os

os.chdir(os.getcwd()+"/data")

now = datetime.datetime.now()

friendship = []
comments = []
likes = []
posts = []



friendshipFile = open("friendships.dat")
commentsFile = open("comments.dat")
likesFile = open("likes.dat")
postsFile = open("posts.dat")


def getDateTimeFrom(string):
    return datetime.datetime.strptime(string, "%Y-%m-%dT%H:%M:%S.%f")

postDate = postsFile.readline()
mydatetime = datetime.datetime.strptime(postDate.split("+")[0], "%Y-%m-%dT%H:%M:%S.%f")
n = input("Digite o fator de velocidade por segundo em segundos: ")
speedFactor = n

print postDate

postDate = postsFile.readline()
likeDate = likesFile.readline()
commentDate = commentsFile.readline()
friendshipDate = friendshipFile.readline()


while True:
    newTime = datetime.datetime.now()
    delta  = datetime.datetime.now() - now
    if delta.total_seconds() > 1:
        mydatetime += datetime.timedelta(seconds = 1 * speedFactor)
        print "\nTHIS IS THE FAKE TIME NOW: " + str(mydatetime)
        print "THIS IS THE REAL TIME NOW: " + str(now) + "\n"
        now = newTime

        while (getDateTimeFrom(postDate.split("+")[0]) - mydatetime).total_seconds() <= 0:
            print "POST: " + postDate[:-1]
            postDate = postsFile.readline()        
        
        while (getDateTimeFrom(likeDate.split("+")[0]) - mydatetime).total_seconds() <= 0:
            print "LIKE: " + likeDate[:-1]
            likeDate = likesFile.readline()

        while (getDateTimeFrom(commentDate.split("+")[0]) - mydatetime).total_seconds() <= 0:
            print "COMMENT: " + commentDate[:-1]
            commentDate = commentsFile.readline()

        while (getDateTimeFrom(friendshipDate.split("+")[0]) - mydatetime).total_seconds() <= 0:
            print "FRIENDSHIP: " + friendshipDate[:-1]
            friendshipDate = friendshipFile.readline()
    
    
