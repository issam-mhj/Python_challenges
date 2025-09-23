from sqlalchemy import create_engine, MetaData, Text, Table, Column,func, Float, Integer, CheckConstraint,or_, String, ForeignKey,DateTime, select, desc, asc


engine = create_engine("postgresql+psycopg2://postgres:Issam%40Issam@localhost:5432/restaurant_db")

metadata = MetaData()

categories = Table(
    "categories",
    metadata,
    Column("id",Integer,primary_key=True),
    Column("nom",String)
    )


plats = Table(
    "plats",
    metadata,
    Column("id",Integer,primary_key=True,autoincrement=True),
    Column("nom",String),
    Column("prix",Float),
    Column("description",String),
    Column("categorie_id",Integer,ForeignKey("categories.id"))
)

clients = Table(
    "clients",
    metadata,
    Column("id",Integer,primary_key=True,autoincrement=True),
    Column("nom",String),
    Column("email",String),
    Column("telephone",String,nullable=True)
    )

commandes = Table(
    "commandes",
    metadata,
    Column("id",Integer,primary_key=True,autoincrement=True),
    Column("client_id",Integer,ForeignKey("clients.id")),
    Column("date_commande",DateTime(timezone=True)),
    Column("total",Float)
)


commandes_plats = Table(
    "commandes_plats",
    metadata,
    Column("commande_id",Integer,ForeignKey("commandes.id")),
    Column("plat_id",Integer,ForeignKey("plats.id")),
    Column("quantite",Integer)
)


fournisseurs = Table(
    "fournisseurs",
    metadata,
    Column("id",Integer,primary_key=True,autoincrement=True),
    Column("nom",String),
    Column("contact",String)
) 

ingredients = Table(
    "ingredients",
    metadata,
    Column("id",Integer,primary_key=True,autoincrement=True),
    Column("nom",String),
    Column("cout_unitaire",Float),
    Column("stock",Float),
    Column("fournisseur_id",Integer,ForeignKey("fournisseurs.id"))
)

plat_ingredients = Table(
    "plat_ingredients",
    metadata,
    Column("plat_id",Integer,ForeignKey("plats.id")),
    Column("ingredient_id",Integer,ForeignKey("ingredients.id")),
    Column("quantite_necessaire",Float)
)

avis = Table(
    "avis",
    metadata,
    Column("id",Integer,primary_key=True,autoincrement=True),
    Column("client_id",Integer,ForeignKey("clients.id")),
    Column("plat_id",Integer,ForeignKey("plats.id")),
    Column("note",Integer,CheckConstraint("note >= 1 AND note <= 5")),
    Column("commentaire",Text,nullable=True),
    Column("date_avis",DateTime(timezone=True))
)

metadata.create_all(engine)

# Insert data only if tables are empty
with engine.connect() as conn:
    # Check if categories table is empty
    result = conn.execute(select(categories.c.id).limit(1))
    if result.fetchone() is None:
        data = [
            {"nom": "Entrée"},
            {"nom": "Plat principal"},
            {"nom": "Dessert"},
            {"nom": "Boisson"},
            {"nom": "Végétarien"}
        ]
        conn.execute(categories.insert(), data)
        conn.commit()

with engine.connect() as conn:
    # Check if plats table is empty
    result = conn.execute(select(plats.c.id).limit(1))
    if result.fetchone() is None:
        data = [
            {"nom": "Salade César", "prix": 45, "description": "Salade avec poulet grillé", "categorie_id": 1},
            {"nom": "Soupe de légumes", "prix": 30, "description": "Soupe chaude de saison", "categorie_id": 1},
            {"nom": "Steak frites", "prix": 90, "description": "Viande grillée et frites", "categorie_id": 2},
            {"nom": "Pizza Margherita", "prix": 70, "description": "Pizza tomate & mozzarella", "categorie_id": 2},
            {"nom": "Tiramisu", "prix": 35, "description": "Dessert italien", "categorie_id": 3},
            {"nom": "Glace 2 boules", "prix": 25, "description": "Glace au choix", "categorie_id": 3},
            {"nom": "Coca-Cola", "prix": 15, "description": "Boisson gazeuse", "categorie_id": 4},
            {"nom": "Eau minérale", "prix": 10, "description": "Eau plate ou gazeuse", "categorie_id": 4},
            {"nom": "Curry de légumes", "prix": 65, "description": "Plat végétarien épicé", "categorie_id": 5},
            {"nom": "Falafel wrap", "prix": 50, "description": "Wrap avec falafels et légumes", "categorie_id": 5}
        ]
        conn.execute(plats.insert(), data)
        conn.commit()

with engine.connect() as conn:
    result = conn.execute(select(clients.c.id).limit(1))
    if result.fetchone() is None:
        data = [
            {"nom": "Amine Lahmidi", "email": "amine@example.com", "telephone": "+212600123456"},
            {"nom": "Sara Benali", "email": "sara.b@example.com", "telephone": "+212600654321"},
            {"nom": "Youssef El Khalfi", "email": "youssef.k@example.com", "telephone": None},
            {"nom": "Fatima Zahra", "email": "fatima.z@example.com", "telephone": "+212600987654"},
            {"nom": "Omar Alaoui", "email": "omar.a@example.com", "telephone": "+212600112233"}
        ]
        conn.execute(clients.insert(), data)
        conn.commit()

with engine.connect() as conn:
    result = conn.execute(select(commandes.c.id).limit(1))
    if result.fetchone() is None:
        data = [
            {"client_id": 1, "date_commande": "2025-07-07 12:30:00", "total": 120},
            {"client_id": 2, "date_commande": "2025-07-07 13:00:00", "total": 85},
            {"client_id": 1, "date_commande": "2025-07-08 19:45:00", "total": 150},
            {"client_id": 3, "date_commande": "2025-08-15 18:30:00", "total": 200},
            {"client_id": 4, "date_commande": "2025-09-01 20:00:00", "total": 95},
            {"client_id": 5, "date_commande": "2025-09-10 12:15:00", "total": 75}
        ]
        conn.execute(commandes.insert(), data)
        conn.commit()

# ✅ COMMANDES_PLATS
with engine.connect() as conn:
    result = conn.execute(select(commandes_plats.c.commande_id).limit(1))
    if result.fetchone() is None:
        data = [
            {"commande_id": 1, "plat_id": 1, "quantite": 1},
            {"commande_id": 1, "plat_id": 3, "quantite": 1},
            {"commande_id": 1, "plat_id": 7, "quantite": 2},
            {"commande_id": 2, "plat_id": 2, "quantite": 1},
            {"commande_id": 2, "plat_id": 4, "quantite": 1},
            {"commande_id": 2, "plat_id": 8, "quantite": 1},
            {"commande_id": 3, "plat_id": 3, "quantite": 1},
            {"commande_id": 3, "plat_id": 5, "quantite": 1},
            {"commande_id": 3, "plat_id": 7, "quantite": 1},
            {"commande_id": 4, "plat_id": 4, "quantite": 2},
            {"commande_id": 4, "plat_id": 9, "quantite": 1},
            {"commande_id": 5, "plat_id": 10, "quantite": 1},
            {"commande_id": 5, "plat_id": 8, "quantite": 2},
            {"commande_id": 6, "plat_id": 7, "quantite": 3},
            {"commande_id": 6, "plat_id": 6, "quantite": 1}
        ]
        conn.execute(commandes_plats.insert(), data)
        conn.commit()

# ✅ FOURNISSEURS
with engine.connect() as conn:
    result = conn.execute(select(fournisseurs.c.id).limit(1))
    if result.fetchone() is None:
        data = [
            {"nom": "AgriFresh", "contact": "contact@agrifresh.com"},
            {"nom": "MeatSupplier", "contact": "info@meatsupplier.com"},
            {"nom": "BevCo", "contact": "sales@bevco.com"},
            {"nom": "DairyFarm", "contact": "dairy@farm.com"}
        ]
        conn.execute(fournisseurs.insert(), data)
        conn.commit()

# ✅ INGREDIENTS
with engine.connect() as conn:
    result = conn.execute(select(ingredients.c.id).limit(1))
    if result.fetchone() is None:
        data = [
            {"nom": "Poulet", "cout_unitaire": 15.00, "stock": 50, "fournisseur_id": 2},
            {"nom": "Laitue", "cout_unitaire": 5.00, "stock": 20, "fournisseur_id": 1},
            {"nom": "Tomate", "cout_unitaire": 3.00, "stock": 30, "fournisseur_id": 1},
            {"nom": "Mozzarella", "cout_unitaire": 10.00, "stock": 15, "fournisseur_id": 4},
            {"nom": "Pomme de terre", "cout_unitaire": 2.00, "stock": 100, "fournisseur_id": 1},
            {"nom": "Café", "cout_unitaire": 20.00, "stock": 5, "fournisseur_id": 3},
            {"nom": "Sucre", "cout_unitaire": 1.50, "stock": 25, "fournisseur_id": 3},
            {"nom": "Pois chiches", "cout_unitaire": 4.00, "stock": 40, "fournisseur_id": 1}
        ]
        conn.execute(ingredients.insert(), data)
        conn.commit()

# ✅ PLAT_INGREDIENTS
with engine.connect() as conn:
    result = conn.execute(select(plat_ingredients.c.plat_id).limit(1))
    if result.fetchone() is None:
        data = [
            {"plat_id": 1, "ingredient_id": 1, "quantite_necessaire": 0.2},
            {"plat_id": 1, "ingredient_id": 2, "quantite_necessaire": 0.1},
            {"plat_id": 2, "ingredient_id": 2, "quantite_necessaire": 0.05},
            {"plat_id": 2, "ingredient_id": 5, "quantite_necessaire": 0.1},
            {"plat_id": 3, "ingredient_id": 1, "quantite_necessaire": 0.3},
            {"plat_id": 3, "ingredient_id": 5, "quantite_necessaire": 0.2},
            {"plat_id": 4, "ingredient_id": 3, "quantite_necessaire": 0.1},
            {"plat_id": 4, "ingredient_id": 4, "quantite_necessaire": 0.15},
            {"plat_id": 5, "ingredient_id": 6, "quantite_necessaire": 0.05},
            {"plat_id": 5, "ingredient_id": 7, "quantite_necessaire": 0.02},
            {"plat_id": 9, "ingredient_id": 8, "quantite_necessaire": 0.1},
            {"plat_id": 10, "ingredient_id": 8, "quantite_necessaire": 0.15}
        ]
        conn.execute(plat_ingredients.insert(), data)
        conn.commit()

# ✅ AVIS
with engine.connect() as conn:
    result = conn.execute(select(avis.c.id).limit(1))
    if result.fetchone() is None:
        data = [
            {"client_id": 1, "plat_id": 1, "note": 4, "commentaire": "Très frais, poulet bien cuit", "date_avis": "2025-07-07 13:00:00"},
            {"client_id": 2, "plat_id": 4, "note": 5, "commentaire": "Meilleure pizza du coin !", "date_avis": "2025-07-07 14:00:00"},
            {"client_id": 3, "plat_id": 9, "note": 3, "commentaire": "Un peu trop épicé", "date_avis": "2025-08-15 19:00:00"},
            {"client_id": 4, "plat_id": 10, "note": 4, "commentaire": "Bon, mais manque de sauce", "date_avis": "2025-09-01 21:00:00"},
            {"client_id": 5, "plat_id": 6, "note": 5, "commentaire": "Glace délicieuse", "date_avis": "2025-09-10 13:00:00"}
        ]
        conn.execute(avis.insert(), data)
        conn.commit()

with engine.connect() as conn :
    que = select(plats.c.nom , plats.c.prix).order_by(desc(plats.c.prix))
    result = conn.execute(que)
    # for row in result : 
    #     print(row) 


with engine.connect() as conn : 
    que = select(plats).where(plats.c.prix.between(30,80))
    result = conn.execute(que)
    # for row in result : 
    #     print(row)


with engine.connect() as conn : 
    que = select(clients).where(or_(
        clients.c.nom.startswith('S'),
        clients.c.nom.startswith('F')
    ))
    result = conn.execute(que)
    # for row in result : 
    #     print(row)


with engine.connect() as conn : 
    que = select(plats.c.nom, categories.c.nom, fournisseurs.c.nom).join(
        categories, plats.c.categorie_id == categories.c.id
    ).join(
        plat_ingredients, plats.c.id == plat_ingredients.c.plat_id
    ).join(
        ingredients, plat_ingredients.c.ingredient_id == ingredients.c.id
    ).join(
        fournisseurs, ingredients.c.fournisseur_id == fournisseurs.c.id
    ).distinct()
    result = conn.execute(que)
    # for row in result : 
        # print(row)
    
with engine.connect() as conn :
    req = select(commandes.c.date_commande,clients.c.nom,func.sum(commandes_plats.c.quantite).label("total_plat")).join(
        clients, clients.c.id == commandes.c.client_id  
    ).join(
        commandes_plats,commandes.c.id == commandes_plats.c.commande_id
    ).group_by(commandes.c.id, commandes.c.date_commande, clients.c.nom)
    result = conn.execute(req)
    # for row in result : 
    #     print(row)

# 8. 

with engine.connect() as conn:
    req = select(
        commandes.c.id.label("commande_id"),
        plats.c.nom.label("plat_nom"),
        commandes_plats.c.quantite,
        func.sum(plat_ingredients.c.quantite_necessaire * ingredients.c.cout_unitaire).label("cout_total_ingredients")
    ).select_from(commandes_plats
    ).join(
        commandes, commandes_plats.c.commande_id == commandes.c.id
    ).join(
        plats, commandes_plats.c.plat_id == plats.c.id
    ).join(
        plat_ingredients, plats.c.id == plat_ingredients.c.plat_id
    ).join(
        ingredients, plat_ingredients.c.ingredient_id == ingredients.c.id
    ).group_by(commandes.c.id, plats.c.nom, commandes_plats.c.quantite)
    result = conn.execute(req)
    # for row in result:
    #     print(row)

# 9) 

with engine.connect() as conn : 
    req = select(categories.c.nom,func.count(plats.c.id).label("count")).outerjoin(
        plats, categories.c.id == plats.c.categorie_id
    ).group_by(categories.c.nom)
    result = conn.execute(req)
    # for row in result:
    #     print(row)

# 10) 

with engine.connect() as conn:
    req = select(
        categories.c.nom.label("categorie"),
        func.avg(plats.c.prix).label("prix_moyen")
    ).join(
        plats, categories.c.id == plats.c.categorie_id
    ).group_by(categories.c.nom)
    result = conn.execute(req)
    # for row in result:
    #     print("Catégorie:", row.categorie, "| Prix moyen des plats:", row.prix_moyen)


with engine.connect() as conn:
    req = select(
        plats.c.nom.label("plat"),
        func.avg(plat_ingredients.c.quantite_necessaire * ingredients.c.cout_unitaire).label("cout_moyen_ingredients")
    ).join(
        plat_ingredients, plats.c.id == plat_ingredients.c.plat_id
    ).join(
        ingredients, plat_ingredients.c.ingredient_id == ingredients.c.id
    ).group_by(plats.c.nom)
    result = conn.execute(req)
    # for row in result:
    #     print("Plat:", row.plat, "| Coût moyen des ingrédients:", row.cout_moyen_ingredients)

# 11)

with engine.connect() as conn : 
    req= select(clients.c.nom,func.count(commandes.c.id).label("total")).join(
        commandes, clients.c.id == commandes.c.client_id
    ).group_by(clients.c.nom).order_by(desc("total"))
    result = conn.execute(req)
    for row in result:
        print(row)

# 12) 
with engine.connect() as conn : 
    total = func.count(commandes.c.id).label("total")
    req = select(clients.c.nom, total).join(
        commandes, clients.c.id == commandes.c.client_id
    ).group_by(clients.c.nom).having(total >= 2).order_by(desc(total))
    result = conn.execute(req)
    for row in result:
        print(row)

# 13.
with engine.connect() as conn:
    req = select(
        plats.c.nom.label("plat"),
        func.sum(commandes_plats.c.quantite).label("total_quantite"),
        func.avg(avis.c.note).label("note_moyenne")
    ).join(
        commandes_plats, plats.c.id == commandes_plats.c.plat_id
    ).outerjoin(
        avis, plats.c.id == avis.c.plat_id
    ).group_by(plats.c.nom)
    req = req.having(func.sum(commandes_plats.c.quantite) > 3)
    result = conn.execute(req)
    for row in result:
        print(row)
# 14. 
from sqlalchemy import extract
with engine.connect() as conn:
    req = select(
        commandes.c.id,
        commandes.c.date_commande,
        clients.c.nom
    ).join(clients, commandes.c.client_id == clients.c.id)
    req = req.where(
        extract('year', commandes.c.date_commande) == 2025,
        extract('month', commandes.c.date_commande).in_([7, 8, 9])
    )
    result = conn.execute(req)
    for row in result:
        print(row)
# 15.
with engine.connect() as conn:
   
    subq = select(commandes.c.id).order_by(desc(commandes.c.date_commande)).limit(1).scalar_subquery()
    req = select(
        commandes.c.id,
        commandes.c.date_commande,
        clients.c.nom.label("client"),
        plats.c.nom.label("plat"),
        commandes_plats.c.quantite
    ).join(clients, commandes.c.client_id == clients.c.id)
    req = req.join(commandes_plats, commandes.c.id == commandes_plats.c.commande_id)
    req = req.join(plats, commandes_plats.c.plat_id == plats.c.id)
    req = req.where(commandes.c.id == subq)
    result = conn.execute(req)
    for row in result:
        print(row)
# 16.
with engine.connect() as conn:
    req = select(
        clients.c.nom,
        clients.c.telephone,
        commandes.c.total
    ).join(commandes, clients.c.id == commandes.c.client_id)
    req = req.where(commandes.c.total > 150)
    result = conn.execute(req)
    for row in result:
        print(row)
# 17. 
with engine.connect() as conn:
    req = select(
        plats.c.nom,
        plats.c.prix,
        func.sum(plat_ingredients.c.quantite_necessaire * ingredients.c.cout_unitaire).label("cout_total_ingredients")
    ).join(
        plat_ingredients, plats.c.id == plat_ingredients.c.plat_id
    ).join(
        ingredients, plat_ingredients.c.ingredient_id == ingredients.c.id
    ).group_by(plats.c.id)
    req = req.having(func.sum(plat_ingredients.c.quantite_necessaire * ingredients.c.cout_unitaire) > (plats.c.prix * 0.5))
    result = conn.execute(req)
    for row in result:
        print(row)
# 18. 
with engine.connect() as conn:
    
    cat_id = conn.execute(select(categories.c.id).where(categories.c.nom == "Végétarien")).scalar()
    
    result = conn.execute(plats.insert().returning(plats.c.id), {
        "nom": "Burger végétarien",
        "prix": 60,
        "description": "Burger végétarien maison",
        "categorie_id": cat_id
    })
    new_plat_id = result.scalar()
    
    ing1 = conn.execute(select(ingredients.c.id).where(ingredients.c.nom == "Laitue")).scalar()
    ing2 = conn.execute(select(ingredients.c.id).where(ingredients.c.nom == "Pois chiches")).scalar()
    conn.execute(plat_ingredients.insert(), [
        {"plat_id": new_plat_id, "ingredient_id": ing1, "quantite_necessaire": 0.12},
        {"plat_id": new_plat_id, "ingredient_id": ing2, "quantite_necessaire": 0.18}
    ])
    conn.commit()
    print(f"Plat végétarien ajouté avec id {new_plat_id}")
# 19.
with engine.connect() as conn:
    
    client_id = conn.execute(select(clients.c.id).where(clients.c.nom == "Youssef El Khalfi")).scalar()
    if client_id:
        
        conn.execute(avis.delete().where(avis.c.client_id == client_id))
       
        order_ids = [row[0] for row in conn.execute(select(commandes.c.id).where(commandes.c.client_id == client_id))]
        
        if order_ids:
            conn.execute(commandes_plats.delete().where(commandes_plats.c.commande_id.in_(order_ids)))
        
        conn.execute(commandes.delete().where(commandes.c.client_id == client_id))
        
        conn.execute(clients.delete().where(clients.c.id == client_id))
        conn.commit()
        print("Client 'Youssef El Khalfi' et ses données supprimés.")
    else:
        print("Client 'Youssef El Khalfi' non trouvé.")
# 20. 
with engine.connect() as conn:
    req = select(
        clients.c.nom,
        func.coalesce(func.sum(commandes_plats.c.quantite), 0).label("total_plats"),
        func.coalesce(func.sum(commandes.c.total), 0).label("total_depense"),
        func.avg(avis.c.note).label("note_moyenne")
    ).outerjoin(commandes, clients.c.id == commandes.c.client_id)
    req = req.outerjoin(commandes_plats, commandes.c.id == commandes_plats.c.commande_id)
    req = req.outerjoin(avis, clients.c.id == avis.c.client_id)
    req = req.group_by(clients.c.id)
    result = conn.execute(req)
    for row in result:
        print(row)
# 21.
with engine.connect() as conn:
    req = select(
        plats.c.nom.label("plat"),
        categories.c.nom.label("categorie"),
        func.sum(commandes_plats.c.quantite).label("total_quantite")
    ).join(categories, plats.c.categorie_id == categories.c.id)
    req = req.join(commandes_plats, plats.c.id == commandes_plats.c.plat_id)
    req = req.group_by(plats.c.id, categories.c.nom)
    req = req.order_by(desc("total_quantite")).limit(3)
    result = conn.execute(req)
    for row in result:
        print(row)
# 22.
with engine.connect() as conn:
    subq = select(
        commandes.c.client_id,
        func.max(commandes.c.date_commande).label("max_date")
    ).group_by(commandes.c.client_id).subquery()
    latest_orders = select(
        commandes.c.id.label("commande_id"),
        commandes.c.client_id,
        commandes.c.date_commande
    ).join(subq, (commandes.c.client_id == subq.c.client_id) & (commandes.c.date_commande == subq.c.max_date)).subquery()
    req = select(
        clients.c.nom.label("client"),
        latest_orders.c.date_commande,
        plats.c.nom.label("plat"),
        commandes_plats.c.quantite
    ).join(latest_orders, clients.c.id == latest_orders.c.client_id)
    req = req.join(commandes_plats, latest_orders.c.commande_id == commandes_plats.c.commande_id)
    req = req.join(plats, commandes_plats.c.plat_id == plats.c.id)
    req = req.order_by(clients.c.nom)
    result = conn.execute(req)
    for row in result:
        print(row)
# 23.

with engine.connect() as conn:
    req = select(
        clients.c.nom.label("client"),
        plats.c.nom.label("plat"),
        commandes_plats.c.quantite,
        commandes.c.date_commande,
        func.avg(avis.c.note).label("note_moyenne")
    ).join(commandes, clients.c.id == commandes.c.client_id)
    req = req.join(commandes_plats, commandes.c.id == commandes_plats.c.commande_id)
    req = req.join(plats, commandes_plats.c.plat_id == plats.c.id)
    req = req.outerjoin(avis, plats.c.id == avis.c.plat_id)
    req = req.group_by(clients.c.nom, plats.c.nom, commandes_plats.c.quantite, commandes.c.date_commande)
    result = conn.execute(req)
    for row in result:
        print(row)
# 24. 

with engine.connect() as conn:
    req = select(
        fournisseurs.c.nom.label("fournisseur"),
        func.sum(ingredients.c.stock * ingredients.c.cout_unitaire).label("cout_total_stock")
    ).join(ingredients, fournisseurs.c.id == ingredients.c.fournisseur_id)
    req = req.where(ingredients.c.stock < 10)
    req = req.group_by(fournisseurs.c.nom)
    result = conn.execute(req)
    for row in result:
        print(row)