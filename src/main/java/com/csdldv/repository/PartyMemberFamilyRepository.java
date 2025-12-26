package com.csdldv.repository;

import com.csdldv.entity.PartyMemberFamily;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface PartyMemberFamilyRepository extends JpaRepository<PartyMemberFamily, String> {

    @Query(value = """
            SELECT a.PARTY_MEMBER_FAMILY_ID
            FROM CSDLDV_PARTY_MEMBER.PARTY_MEMBER_FAMILY a
            WHERE a.TYPE IS NULL
              AND EXISTS (
                  SELECT 1
                  FROM CSDLDV_20.qhe_gd b
                  WHERE b.guidkey = a.V3_QUANHE_GD_GUID
                    AND b.MA_TV_GD IN (
                        'A2','A3','B5','B6','C7','C8',
                        'E2','E3','M2','M3','R1','R2'
                    )
              )
            ORDER BY a.PARTY_MEMBER_FAMILY_ID
            """, nativeQuery = true)
    List<String> findAllIdsForTypeOneFrom20();

    @Query(value = """
            SELECT a.PARTY_MEMBER_FAMILY_ID
            FROM CSDLDV_PARTY_MEMBER.PARTY_MEMBER_FAMILY a
            WHERE a.TYPE IS NULL
              AND EXISTS (
                  SELECT 1
                  FROM CSDLDV_25.qhe_gd b
                  WHERE b.guidkey = a.V3_QUANHE_GD_GUID
                    AND b.MA_TV_GD IN (
                        'A2','A3','B5','B6','C7','C8',
                        'E2','E3','M2','M3','R1','R2'
                    )
              )
            ORDER BY a.PARTY_MEMBER_FAMILY_ID
            """, nativeQuery = true)
    List<String> findAllIdsForTypeOneFrom25();

    @Query(value = """
            SELECT a.PARTY_MEMBER_FAMILY_ID
            FROM CSDLDV_PARTY_MEMBER.PARTY_MEMBER_FAMILY a
            WHERE a.TYPE IS NULL
              AND EXISTS (
                  SELECT 1
                  FROM CSDLDV_26.qhe_gd b
                  WHERE b.guidkey = a.V3_QUANHE_GD_GUID
                    AND b.MA_TV_GD IN (
                        'A2','A3','B5','B6','C7','C8',
                        'E2','E3','M2','M3','R1','R2'
                    )
              )
            ORDER BY a.PARTY_MEMBER_FAMILY_ID
            """, nativeQuery = true)
    List<String> findAllIdsForTypeOneFrom26();

    @Modifying
    @Query(value = """
            UPDATE CSDLDV_PARTY_MEMBER.PARTY_MEMBER_FAMILY
            SET TYPE = 1
            WHERE PARTY_MEMBER_FAMILY_ID IN :ids
            """, nativeQuery = true)
    int bulkSetTypeOne(List<String> ids);

    @Modifying
    @Query(value = """
            UPDATE CSDLDV_PARTY_MEMBER.PARTY_MEMBER_FAMILY
            SET TYPE = 0
            WHERE TYPE IS NULL
            """, nativeQuery = true)
    int bulkSetTypeZero();
}
