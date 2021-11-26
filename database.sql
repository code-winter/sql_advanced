create table if not exists artists(
	artist_id serial primary key,
	artist_name varchar(60)
);

create table if not exists genre(
	genre_id serial primary key,
	genre_name varchar(60) unique not null
);

create table if not exists artist_genre(
	a_g_id serial primary key,
	artist integer not null references artists(artist_id),
	genre integer not null references genre(genre_id)
);

create table if not exists albums(
	album_id serial primary key,
	album_name varchar(60) not null,
	album_release_year integer not null
);

create table if not exists albums_artists(
	a_a_id serial primary key,
	artist integer not null references artists(artist_id),
	album integer not null references albums(album_id)
);

create table if not exists tracks(
	track_id serial primary key,
	track_title varchar(60) not null,
	track_runtime integer not null,
	album integer references albums(album_id)
);

create table if not exists comps(
	comp_id serial primary key,
	comp_name varchar(60) not null,
	comp_release_date integer not null
);

create table if not exists comps_tracks(
	c_t_id serial primary key,
	comp integer not null references comps(comp_id),
	track integer not null references tracks(track_id)
);
