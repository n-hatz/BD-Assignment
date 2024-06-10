# BD-Assignment

Νικήτας Χατζής - 03400237 <br />
Κωσταντίνος Χριστογεώργος - 03400239 <br />


Για να μπορεί να τρέξει ο κώδικας απαιτούνται τα παρακάτω βήματα:
1. Εγκατάσταση spark και hdfs με βάση τις οδηγίες που δόθηκαν στο εργαστήριο 1. Στις οδηγίες αυτές συμπεριλαμβάνεται και η εγκατάσταση της python 3.8.
2. Εγκατάσταση geopy με την εντολή  
   ```
   python3.9 -m pip install geopy
   ```
4. Δημιουργία directory στο hdfs, φόρτωση αρχείων στο VM (π.χ. με SCP) και μεταφορά αρχείων στο hdfs με βάση τις οδηγίες που υπάρχουν στο report (Ζητούμενο 2)
5. Μεταφορά αρχείων κώδικα σε κάποιο folder στο VM (π.χ. με SCP)
6. Για να τρέξει ο κώδικας που μετατρέπει τα ```.csv``` αρχεία σε parquet files:
   ```
   spark-submit csv2parquet.py
   ```
7.  Για να τρέξει κάποιο αρχείο κώδικα
   ```
   spark-submit <file_name>.py
   ```
   
