In this folder you should add the oracle information to the actor. Note that the oracle algorithm is purely academic as in practise a query engine will never have access to this data.

Oracle information should be a JSON object with as $keys$ the urls and $values$ the RCC (Result Contribution Counter). Only documents with RCC > 0 have to be added, as any document not in this RCC file with get a priority of 0.
