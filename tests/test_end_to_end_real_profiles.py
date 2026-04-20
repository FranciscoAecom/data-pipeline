import unittest
import shutil
from pathlib import Path
from types import SimpleNamespace

import geopandas as gpd
import pandas as pd
from shapely.geometry import Point

from core.record_processor import process_record


def _record(input_path, theme_folder, rule_profile, theme):
    return SimpleNamespace(
        sheet_row=2,
        record_id=10,
        theme=theme,
        theme_folder=theme_folder,
        status="Waiting Update",
        source_path=input_path,
        input_path=input_path,
        rule_profile=rule_profile,
    )


class EndToEndRealProfilesTests(unittest.TestCase):
    def test_auth_supn_real_profile_processes_dates_and_persists_output(self):
        temp_dir = Path("tests") / "_tmp_e2e_auth_supn"
        shutil.rmtree(temp_dir, ignore_errors=True)
        temp_dir.mkdir(parents=True, exist_ok=True)
        try:
            input_path = str(temp_dir / "auth_supn_input.gpkg")
            output_dir = str(temp_dir / "output")
            source_gdf = gpd.GeoDataFrame(
                {
                    "sdb_descriptio": ["PMFS/POA"],
                    "sdb_transparen": ["Ativa"],
                    "sdb_jurisdicti": ["Federal"],
                    "sdb_author_dat": ["31/01/2025"],
                    "sdb_expira_dat": ["2026-02-01 00:00:00"],
                    "sdb_dat_d_base": ["15/03/2025"],
                    "geometry": [Point(0, 0)],
                },
                geometry="geometry",
                crs="EPSG:4674",
            )
            source_gdf.to_file(input_path, driver="GPKG")

            result = process_record(
                _record(
                    input_path=input_path,
                    theme_folder="auth_supn",
                    rule_profile="autorizacao_para_supressao_vegetal/auth_supn",
                    theme="Autorizacao para Supressao Vegetal",
                ),
                output_dir=output_dir,
                use_configured_final_name=True,
                persist_individual_output=True,
            )

            self.assertEqual(result.processed_count, 1)
            self.assertIsNotNone(result.output_path)
            self.assertTrue(Path(result.output_path).exists())
            self.assertIn("acm_author_dat", result.final_gdf.columns)
            self.assertIn("acm_expira_dat", result.final_gdf.columns)
            self.assertIn("acm_dat_d_base", result.final_gdf.columns)
            self.assertEqual(
                result.final_gdf.loc[0, "acm_author_dat"],
                pd.Timestamp("2025-01-31"),
            )
            self.assertEqual(
                result.final_gdf.loc[0, "acm_expira_dat"],
                pd.Timestamp("2026-02-01"),
            )
        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)

    def test_reserva_legal_real_profile_transforms_and_normalizes_fields(self):
        temp_dir = Path("tests") / "_tmp_e2e_rl_car"
        shutil.rmtree(temp_dir, ignore_errors=True)
        temp_dir.mkdir(parents=True, exist_ok=True)
        try:
            input_path = str(temp_dir / "rl_car_input.gpkg")
            output_dir = str(temp_dir / "output")
            source_gdf = gpd.GeoDataFrame(
                {
                    "sdb_cod_tema": ["ARL_AVERBADA"],
                    "sdb_nom_tema": ["Reserva Legal Averbada"],
                    "sdb_ind_status": ["AT"],
                    "sdb_des_condic": ["Analisado sem pendencias"],
                    "geometry": [Point(1, 1)],
                },
                geometry="geometry",
                crs="EPSG:4674",
            )
            source_gdf.to_file(input_path, driver="GPKG")

            result = process_record(
                _record(
                    input_path=input_path,
                    theme_folder="rl_car_ac",
                    rule_profile="reserva_legal_car/rl_car_ac",
                    theme="Reserva Legal",
                ),
                output_dir=output_dir,
                use_configured_final_name=True,
                persist_individual_output=True,
            )

            self.assertEqual(result.processed_count, 1)
            self.assertIsNotNone(result.output_path)
            self.assertTrue(Path(result.output_path).exists())
            self.assertIn("sdb_cod_tema", result.final_gdf.columns)
            self.assertIn("sdb_nom_tema", result.final_gdf.columns)
            self.assertIn("sdb_ind_status", result.final_gdf.columns)
            self.assertIn("acm_des_condic", result.final_gdf.columns)
            self.assertEqual(result.final_gdf.loc[0, "sdb_cod_tema"], "ARL_AVERBADA")
            self.assertEqual(
                result.final_gdf.loc[0, "sdb_nom_tema"],
                "Reserva Legal Averbada",
            )
            self.assertEqual(result.final_gdf.loc[0, "sdb_ind_status"], "AT")
            self.assertEqual(result.final_gdf.loc[0, "acm_des_condic"], "Analisado")
        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)
