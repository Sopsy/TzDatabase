<?php
declare(strict_types=1);

namespace TzDatabase\Repository;

use PDO;

final class TzDatabase
{
    public function __construct(
        private readonly PDO $db,
        private readonly string $fallbackTimezone = 'UTC'
    ) {
    }

    public function tzByLocation(string $countryCode, string $regionName, string $cityName): string
    {
        $q = $this->db->prepare(
            'SELECT tz FROM tz_database
            WHERE country_code = :country_code AND region_name = :region_name AND city_name = :city_name LIMIT 1'
        );
        $q->bindValue(':country_code', $countryCode);
        $q->bindValue(':region_name', $regionName);
        $q->bindValue(':city_name', $cityName);
        $q->execute();

        return (string)($q->fetchColumn() ?: $this->fallbackTimezone);
    }

    public function update(array $rows): void
    {
        $this->db->exec('BEGIN');

        // No truncate table because it does an implicit commit
        $this->db->exec('DELETE FROM tz_database WHERE 1');

        foreach ($rows as $row) {
            if (
                empty($row['country_code']) ||
                empty($row['region_name']) ||
                empty($row['city_name']) ||
                empty($row['olson_tz'])
            ) {
                continue;
            }
            // Yes, separate queriers for each row so the query is not HUGE!
            $q = $this->db->prepare(
                'INSERT INTO tz_database (country_code, region_name, city_name, tz)
                VALUES (:country_code, :region_name, :city_name, :tz)'
            );
            $q->bindValue(':country_code', $row['country_code']);
            $q->bindValue(':region_name', $row['region_name']);
            $q->bindValue(':city_name', $row['city_name']);
            $q->bindValue(':tz', $row['olson_tz']);
            $q->execute();
        }

        $this->db->exec('COMMIT');
    }
}