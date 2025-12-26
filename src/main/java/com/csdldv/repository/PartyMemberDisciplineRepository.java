package com.csdldv.repository;

import com.csdldv.entity.PartyMemberDiscipline;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;
import java.util.List;

@Repository
public interface PartyMemberDisciplineRepository extends JpaRepository<PartyMemberDiscipline, String> {

    @Query(value = """
            SELECT x.PARTY_MEMBER_DISCIPLINE_ID
            FROM CSDLDV_PARTY_MEMBER.PARTY_MEMBER_DISCIPLINE x
            WHERE EXISTS (
                SELECT 1
                FROM CSDLDV_20.KT_KL kl
                JOIN CSDLDV_CATEGORY.CATEGORY c
                  ON ('0' || c.CATEGORY_CODE) = kl.MA_KL
                WHERE c.CATEGORY_GROUP_CODE = 'HINHTHUCKYLUAT'
                  AND kl.GUIDKEY = x.V3_KT_KL_GUID
            )
            ORDER BY x.PARTY_MEMBER_DISCIPLINE_ID
            """, nativeQuery = true)
    List<String> findAllIdsForUpdateFrom20();

    @Query(value = """
            SELECT x.PARTY_MEMBER_DISCIPLINE_ID
            FROM CSDLDV_PARTY_MEMBER.PARTY_MEMBER_DISCIPLINE x
            WHERE EXISTS (
                SELECT 1
                FROM CSDLDV_25.KT_KL kl
                JOIN CSDLDV_CATEGORY.CATEGORY c
                  ON ('0' || c.CATEGORY_CODE) = kl.MA_KL
                WHERE c.CATEGORY_GROUP_CODE = 'HINHTHUCKYLUAT'
                  AND kl.GUIDKEY = x.V3_KT_KL_GUID
            )
            ORDER BY x.PARTY_MEMBER_DISCIPLINE_ID
            """, nativeQuery = true)
    List<String> findAllIdsForUpdateFrom25();

    @Query(value = """
            SELECT x.PARTY_MEMBER_DISCIPLINE_ID
            FROM CSDLDV_PARTY_MEMBER.PARTY_MEMBER_DISCIPLINE x
            WHERE EXISTS (
                SELECT 1
                FROM CSDLDV_26.KT_KL kl
                JOIN CSDLDV_CATEGORY.CATEGORY c
                  ON ('0' || c.CATEGORY_CODE) = kl.MA_KL
                WHERE c.CATEGORY_GROUP_CODE = 'HINHTHUCKYLUAT'
                  AND kl.GUIDKEY = x.V3_KT_KL_GUID
            )
            ORDER BY x.PARTY_MEMBER_DISCIPLINE_ID
            """, nativeQuery = true)
    List<String> findAllIdsForUpdateFrom26();

    @Modifying
    @Query(value = """
            UPDATE CSDLDV_PARTY_MEMBER.PARTY_MEMBER_DISCIPLINE x
            SET (x.DISCIPLINE_FORM_ID, x.DISCIPLINE_REASON) =
            (
                SELECT c.CATEGORY_ID, kl.LYDO
                FROM CSDLDV_CATEGORY.CATEGORY c
                JOIN CSDLDV_20.KT_KL kl
                  ON ('0' || c.CATEGORY_CODE) = kl.MA_KL
                WHERE c.CATEGORY_GROUP_CODE = 'HINHTHUCKYLUAT'
                  AND kl.GUIDKEY = x.V3_KT_KL_GUID
            )
            WHERE x.PARTY_MEMBER_DISCIPLINE_ID IN :ids
            """, nativeQuery = true)
    int bulkUpdateDisciplineFormAndReasonFrom20(List<String> ids);

    @Modifying
    @Query(value = """
            UPDATE CSDLDV_PARTY_MEMBER.PARTY_MEMBER_DISCIPLINE x
            SET (x.DISCIPLINE_FORM_ID, x.DISCIPLINE_REASON) =
            (
                SELECT c.CATEGORY_ID, kl.LYDO
                FROM CSDLDV_CATEGORY.CATEGORY c
                JOIN CSDLDV_25.KT_KL kl
                  ON ('0' || c.CATEGORY_CODE) = kl.MA_KL
                WHERE c.CATEGORY_GROUP_CODE = 'HINHTHUCKYLUAT'
                  AND kl.GUIDKEY = x.V3_KT_KL_GUID
            )
            WHERE x.PARTY_MEMBER_DISCIPLINE_ID IN :ids
            """, nativeQuery = true)
    int bulkUpdateDisciplineFormAndReasonFrom25(List<String> ids);

    @Modifying
    @Query(value = """
            UPDATE CSDLDV_PARTY_MEMBER.PARTY_MEMBER_DISCIPLINE x
            SET (x.DISCIPLINE_FORM_ID, x.DISCIPLINE_REASON) =
            (
                SELECT c.CATEGORY_ID, kl.LYDO
                FROM CSDLDV_CATEGORY.CATEGORY c
                JOIN CSDLDV_26.KT_KL kl
                  ON ('0' || c.CATEGORY_CODE) = kl.MA_KL
                WHERE c.CATEGORY_GROUP_CODE = 'HINHTHUCKYLUAT'
                  AND kl.GUIDKEY = x.V3_KT_KL_GUID
            )
            WHERE x.PARTY_MEMBER_DISCIPLINE_ID IN :ids
            """, nativeQuery = true)
    int bulkUpdateDisciplineFormAndReasonFrom26(List<String> ids);
}
