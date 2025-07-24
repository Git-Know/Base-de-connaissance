CREATE CONSTRAINT IF NOT EXISTS unique_developer_email FOR (d:Developer) REQUIRE d.email IS UNIQUE;
CREATE CONSTRAINT IF NOT EXISTS unique_project_name FOR (p:Project) REQUIRE p.name IS UNIQUE;
CREATE CONSTRAINT IF NOT EXISTS unique_technology_name FOR (t:Technology) REQUIRE t.name IS UNIQUE;
