#  Tema 2 APD: Map-Reduce
## Burdiniuc Ilie, 336CB

- Am folosit 2 ExecutorService pentru a rula taksurile de tip map si reduce pe un numar de thread-uri specificate.
- Fiecare task, de tip reduce sau map proceseaza fisierul lui.
- MapTask citea din fisier de la un offset un numar de biti, apoi verifica daca taie un cuvant in 2 parti, daca da
atunci sfarsitul acestui cuvant il concatena la acest string. Apoi face un vector de cuvinte si calculeaza lungimea
si adauga in dictionar. La finalul unui task rezulta o lista cu dictionare create de fiecare task.
- Dupa ce au terminat toate task-urile de tip map, creez task-urile de tip reduce care primesc ca parametrii rezultatele
partiale rezultate din map tasks.
- Uneste toate dictionarele din lista in unul final, ca sa fie toate datele la gramada.
- Parcurge dictionarul final si folosind formula calculeaza: suma (rezultatul functiei fibonacci din lungimea cuvantului
si numarul de aparitii) si imparte la numarul de cuvinte pentru a afla rangul documentului. In acelasi for se afla si
cel mai lung cuvant si se salveaza lungimea acestuia si numarul lui de aparitii.
- Dupa ce s-au terminat toate task-urile de tip reduce, acestia sunt sortati dupa rang in ordine descrescatoare si
si scrisi in fisier de out.
