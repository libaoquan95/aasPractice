Music Listening Dataset
Audioscrobbler.com
6 May 2005
--------------------------------

This data set contains profiles for around 150,000 real people
The dataset lists the artists each person listens to, and a counter
indicating how many times each user played each artist

The dataset is continually growing; at the time of writing (6 May 2005) 
Audioscrobbler is receiving around 2 million song submissions per day

We may produce additional/extended data dumps if anyone is interested 
in experimenting with the data. 

Please let us know if you do anything useful with this data, we're always
up for new ways to visualize it or analyse/cluster it etc :)


License
-------

This data is made available under the following Creative Commons license:
http://creativecommons.org/licenses/by-nc-sa/1.0/


Files
-----

user_artist_data.txt
    3 columns: userid artistid playcount

artist_data.txt
    2 columns: artistid artist_name

artist_alias.txt
    2 columns: badid, goodid
    known incorrectly spelt artists and the correct artist id. 
    you can correct errors in user_artist_data as you read it in using this file
    (we're not yet finished merging this data)
    
    
Contact Info
------------
rj@audioscrobbler.com
irc://irc.audioscrobbler.com/audioscrobbler
