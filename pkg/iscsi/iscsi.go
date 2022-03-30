package iscsi

import (
	"fmt"

	"github.com/fightdou/os-brick-rbd/pkg/utils"
	"github.com/wonderivan/logger"
)

//LoginPortal login iscsi partal
func LoginPortal(portal string, iqn string) error {
	_, err := utils.ExecIscsiadm(portal, iqn, []string{"--login"})
	if err != nil {
		logger.Error("Exec iscsiadm login command failed", err)
		return err
	}
	_, err = utils.UpdateIscsiadm(portal, iqn, "node.startup", "automatic", nil)
	if err != nil {
		logger.Error("Exec iscsiadm update command failed", err)
		return err
	}
	logger.Info("iscsiadm portal %s login success", portal)
	return nil
}

//disconnectFromIscsiPortal login iscsi partal
func disconnectFromIscsiPortal(portal string, iqn string) error {
	_, err := utils.UpdateIscsiadm(portal, iqn, "node.startup", "manual", nil)
	if err != nil {
		return fmt.Errorf("failed to update node.startup to manual: %w", err)
	}
	_, err = utils.ExecIscsiadm(portal, iqn, []string{"--logout"})
	if err != nil {
		logger.Error("Exec iscsiadm login command failed", err)
		return err
	}
	_, err = utils.ExecIscsiadm(portal, iqn, []string{"--op", "delete"})
	if err != nil {
		return fmt.Errorf("failed to execute --op delete: %w", err)
	}
	logger.Info("iscsiadm portal %s logout success", portal)
	return nil
}
